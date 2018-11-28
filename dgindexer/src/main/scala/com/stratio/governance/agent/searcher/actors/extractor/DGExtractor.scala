package com.stratio.governance.agent.searcher.actors.extractor

import java.sql.{Connection, ResultSet, Statement}
import java.time.Instant

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import com.stratio.governance.agent.searcher.actors.dao.DataAssetDaoWrapper
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractor._
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer
import com.stratio.governance.commons.agent.domain.dao.DataAssetDao
import org.apache.commons.dbcp.DelegatingConnection
import org.json4s.DefaultFormats
import org.postgresql.PGConnection
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.dispatch.Mailbox

object DGExtractor {

  abstract class Message {
    def limit: Long
  }

  case class TotalIndexationMessage(readModifiedSince: Option[Instant], limit: Long, exponentialBackOff: ExponentialBackOff) extends Message
  case class PartialIndexationMessage(readModifiedSince: Option[Instant], limit : Long, exponentialBackOff: ExponentialBackOff) extends Message
  case class SendBatchToIndexerMessage(t: (Array[DataAssetDao], Instant), continue: Option[Message], exponentialBackOff: ExponentialBackOff)
}

class DGExtractor(indexer: ActorRef, params: DGExtractorParams) extends Actor {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)
  implicit val formats: DefaultFormats.type = DefaultFormats

  // execution context for the notifications
  context.dispatcher

  val connection: Connection = ConnectionPool.borrow()
  val db: DB = DB(connection)
  val pgConnection: PGConnection = connection.asInstanceOf[DelegatingConnection].getInnermostDelegate.asInstanceOf[PGConnection]

  //implicit val formats = DefaultFormats

  def setEOnErrorState(): Unit = {
    LOG.error("Circuit breaker open")
    context.become(error)
  }

  def backToNormalState(): Unit = {
    LOG.debug("Circuit breaker half Open")
    context.unbecome()
  }

  def backToClosedState(): Unit = {
    LOG.debug("Circuit breaker Closed")
    context.unbecome()
  }


  override def preStart(): Unit = {
    // make sure connection isn't closed when executing queries
    // we setup the
    db.autoClose(false)
    db.localTx { implicit session =>
      session.connection.setAutoCommit(false)
      val stmt: Statement = session.connection.createStatement(ResultSet.CONCUR_READ_ONLY,
        ResultSet.FETCH_FORWARD,
        ResultSet.TYPE_FORWARD_ONLY)
      stmt.setFetchSize(1000)
      stmt.setMaxRows(1000)

      stmt.execute("LISTEN events")

    }
    context.watch(self)
  }

  override def postStop(): Unit = {
    db.close()
  }

  def receive: PartialFunction[Any, Unit] = {

    case DGExtractor.TotalIndexationMessage(instantRead, limit, exponentialBackOff: ExponentialBackOff) =>
      val results: (Array[DataAssetDao], Instant) = DataAssetDaoWrapper.readDataAssetsSince(instantRead.getOrElse(Instant.MIN), limit)
      if (results._1.size == limit) {
        self ! SendBatchToIndexerMessage(results, Some(DGExtractor.TotalIndexationMessage(Some(results._2), limit, exponentialBackOff)), exponentialBackOff)
      } else if (results._1.nonEmpty) {
        self ! SendBatchToIndexerMessage(results, None, exponentialBackOff)
      }

    case DGExtractor.PartialIndexationMessage(instantRead, limit, exponentialBackOff) =>
      val instant = instantRead.getOrElse(DataAssetDaoWrapper.readLastUpdatedInstant().getOrElse(Instant.MIN))
      val results:(Array[DataAssetDao], Instant) = DataAssetDaoWrapper.readDataAssetsSince(instant, limit)
      if (results._1.size == limit) {
        self ! SendBatchToIndexerMessage(results, Some(DGExtractor.PartialIndexationMessage(Some(instant), limit, exponentialBackOff)), exponentialBackOff)
      } else if (results._1.nonEmpty) {
        self ! SendBatchToIndexerMessage(results, None, exponentialBackOff)
      }

    case SendBatchToIndexerMessage(tuple: (Array[DataAssetDao], Instant), continue: Option[Message], exponentialBackOff: ExponentialBackOff) =>
      (indexer ? DGIndexer.IndexerEvent(tuple._1)).onComplete{
        case Success(_) =>
          DataAssetDaoWrapper.writeLastUpdatedInstant(tuple._2)
          continue match {
            case Some(_) => self ! continue
            case None =>
              if (params.schedulerMode == SchedulerMode.Continuous) {
                context.system.scheduler.scheduleOnce(params.delayMs millis, self, PartialIndexationMessage(Some(tuple._2), params.limit, params.createExponentialBackOff))
              }
          }
        case Failure(e) =>
          //TODO manage errors
          println(s"Indexation failed")
          e.printStackTrace()
          Thread.sleep(exponentialBackOff.getPause)
          self ! SendBatchToIndexerMessage(tuple, continue, exponentialBackOff.next)
      }
  }


  def error: Receive = {
    case msg: AnyRef => LOG.debug(s"Actor in error state no messages processed: ${msg.getClass.getCanonicalName}")
  }
}

