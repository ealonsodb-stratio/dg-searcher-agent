package com.stratio.governance.agent.searcher.actors

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, ActorSystem, DeadLetter, Props}
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractor.{PartialIndexationMessage, TotalIndexationMessage}
import com.stratio.governance.agent.searcher.actors.extractor.{DGExtractorParams, SchedulerMode}
import com.stratio.governance.agent.searcher.actors.indexer.IndexerParams
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class SearcherActorSystem[A <: Actor,B <: Actor](name: String, extractor: Class[A], indexer: Class[B], extractorParams: DGExtractorParams, indexerParams: IndexerParams) {

  def config: Config = load(parseString(
    """
    bounded-mailbox {
      mailbox-type = "akka.dispatch.BoundedMailbox"
      mailbox-capacity = 1
      mailbox-push-timeout-time = 1ms
    }
    """))

  val system = ActorSystem(name, config)
  //system.eventStream.subscribe(system.actorOf(Props(classOf[DeadLetterMetricsActor])), classOf[DeadLetter])
  val indexerRef: ActorRef = system.actorOf(Props(indexer, indexerParams), name + "_indexer")
  var extractorRef: ActorRef = system.actorOf(Props(extractor, indexerRef, extractorParams).withMailbox("bounded-mailbox"), name + "_extractor")

  def performTotalIndexation(): Unit = {
    system.scheduler.scheduleOnce(extractorParams.delayMs millis, extractorRef,  TotalIndexationMessage(None, extractorParams.limit, extractorParams.createExponentialBackOff))
  }

  def initPartialIndexation(): Unit = {
    if (extractorParams.schedulerMode== SchedulerMode.Periodic) {
      system.scheduler.schedule(extractorParams.delayMs millis, extractorParams.periodMs millis, extractorRef, PartialIndexationMessage(None, extractorParams.limit, extractorParams.createExponentialBackOff))
    } else {
      system.scheduler.scheduleOnce(extractorParams.delayMs millis, extractorRef, PartialIndexationMessage(None, extractorParams.limit, extractorParams.createExponentialBackOff))
    }
  }

  object DeadLetterMetricsActor {
    val deadLetterCount = new AtomicInteger
  }

  class DeadLetterMetricsActor extends Actor {
    def receive = {
      case _: DeadLetter => DeadLetterMetricsActor.deadLetterCount.incrementAndGet()
    }
  }
}