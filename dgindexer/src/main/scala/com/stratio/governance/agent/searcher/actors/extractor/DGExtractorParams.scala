package com.stratio.governance.agent.searcher.actors.extractor

import com.stratio.governance.agent.searcher.actors.dao.SourceDao
import com.stratio.governance.agent.searcher.actors.extractor
import com.stratio.governance.agent.searcher.actors.extractor.SchedulerMode.SchedulerMode
import com.stratio.governance.agent.searcher.main.AppConf

object SchedulerMode extends Enumeration {
  type SchedulerMode = Value
  val Periodic: Value = Value("periodic")
  val Continuous: Value = Value("continuous")
  def valueOf(name: String): Option[extractor.SchedulerMode.Value] = SchedulerMode.values.find(_.toString == name)
  def valueOf(name: Option[String]): Option[extractor.SchedulerMode.Value] = name match {
    case Some(_) => SchedulerMode.values.find(_.toString == name.get)
    case None => None
  }
}

class DGExtractorParams(val sourceDao: SourceDao, val limit: Int, val periodMs : Long, val schedulerMode: SchedulerMode, val pauseMs: Long, val maxErrorRetry: Int, val delayMs: Long) {



  def createExponentialBackOff : ExponentialBackOff = ExponentialBackOff(pauseMs, pauseMs, maxErrorRetry, maxErrorRetry)

}
