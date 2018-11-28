package com.stratio.governance.agent.searcher.actors.extractor

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

class DGExtractorParams {

  val LIMIT_LABEL: String = "extractor.limit"
  val DEFAULT_LIMIT: Long = 1000
  val limit: Long = AppConf.getOptionalLong(LIMIT_LABEL).getOrElse(DEFAULT_LIMIT)

  val PERIOD_MS_LABEL: String = "extractor.period.ms"
  val DEFAULT_PERIOD_MS: Long = 10000
  val periodMs : Long = AppConf.getOptionalLong(PERIOD_MS_LABEL).getOrElse(DEFAULT_PERIOD_MS)

  val SCHEDULER_MODE_LABEL: String = "extractor.scheduler.mode"
  val DEFAULT_SCHEDULER_MODE: SchedulerMode = SchedulerMode.Periodic
  val schedulerMode: SchedulerMode = SchedulerMode.valueOf(AppConf.getOptionalString(SCHEDULER_MODE_LABEL)).getOrElse[SchedulerMode](DEFAULT_SCHEDULER_MODE)

  val PAUSE_MS_LABEL: String = "extractor.exponentialbackoff.pause.ms"
  val DEFAULT_PAUSE_MS: Long = 1000
  val pauseMs: Long = AppConf.getOptionalLong(PAUSE_MS_LABEL).getOrElse(DEFAULT_PAUSE_MS)

  val MAX_ERROR_RETRY_LABEL: String = "extractor.exponentialbackoff.maxErrorRetry"
  val DEFAULT_MAX_ERROR_RETRY = 5
  val maxErrorRetry: Int = AppConf.getOptionalInt(MAX_ERROR_RETRY_LABEL).getOrElse(DEFAULT_MAX_ERROR_RETRY)

  val DELAY_MS_LABEL: String = "extractor.delay.ms"
  val DEFAULT_DELAY_MS = 1000
  val delayMs: Long = AppConf.getOptionalLong(DELAY_MS_LABEL).getOrElse(DEFAULT_DELAY_MS)

  def createExponentialBackOff : ExponentialBackOff = ExponentialBackOff(pauseMs, pauseMs, maxErrorRetry, maxErrorRetry)

}
