package com.stratio.governance.agent.searcher.actors.extractor

import java.time.Instant

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractor._
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer
import com.stratio.governance.commons.agent.domain.dao.DataAssetDao
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object DGExtractor {

  abstract class Message {
    def limit: Int
  }

  case class TotalIndexationMessage(readModifiedSince: Option[Instant], limit: Int, exponentialBackOff: ExponentialBackOff) extends Message
  case class PartialIndexationMessage(readModifiedSince: Option[Instant], limit : Int, exponentialBackOff: ExponentialBackOff) extends Message
  case class SendBatchToIndexerMessage(t: (Array[DataAssetDao], Option[Instant]), continue: Option[Message], exponentialBackOff: ExponentialBackOff)
}

class DGExtractor(indexer: ActorRef, params: DGExtractorParams) extends Actor {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)
  implicit val formats: DefaultFormats.type = DefaultFormats

  // execution context for the notifications
  //context.dispatcher



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
    params.sourceDao.preStart()
    context.watch(self)
  }

  override def postStop(): Unit = {
    params.sourceDao.postStop()

  }

  def receive: PartialFunction[Any, Unit] = {

    case DGExtractor.TotalIndexationMessage(instantRead, limit, exponentialBackOff: ExponentialBackOff) =>
      val results: (Array[DataAssetDao], Option[Instant]) = params.sourceDao.readDataAssetsSince(instantRead, limit)
      if (results._1.length == limit) {
        self ! SendBatchToIndexerMessage(results, Some(DGExtractor.TotalIndexationMessage(results._2, limit, exponentialBackOff)), exponentialBackOff)
      } else {
        self ! SendBatchToIndexerMessage(results, None, exponentialBackOff)
      }

    case DGExtractor.PartialIndexationMessage(instantRead, limit, exponentialBackOff) =>
      val instant = instantRead.orElse(params.sourceDao.readLastIngestedInstant())
      val results:(Array[DataAssetDao], Option[Instant]) = params.sourceDao.readDataAssetsSince(instant, limit)
      if (results._1.length == limit) {
        self ! SendBatchToIndexerMessage(results, Some(DGExtractor.PartialIndexationMessage(results._2, limit, exponentialBackOff)), exponentialBackOff)
      } else {
        self ! SendBatchToIndexerMessage(results, None, exponentialBackOff)
      }

    case SendBatchToIndexerMessage(tuple: (Array[DataAssetDao], Option[Instant]), continue: Option[Message], exponentialBackOff: ExponentialBackOff) =>
      (indexer ? DGIndexer.IndexerEvent(tuple._1)).onComplete{
        case Success(_) =>
          params.sourceDao.writeLastIngestedInstant(tuple._2)
          continue match {
            case Some(_) => self ! continue.get
            case None =>
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

