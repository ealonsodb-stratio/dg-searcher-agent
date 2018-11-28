package com.stratio.governance.agent.searcher.main

import com.typesafe.config.ConfigFactory

object AppConf {

  private lazy val config = ConfigFactory.load

  def getOptionalLong(path: String): Option[Long] = {
    if (config.hasPath(path)) {
      Some(config.getLong(path))
    } else {
      None
    }
  }

  def getOptionalInt(path: String): Option[Int] = {
    if (config.hasPath(path)) {
      Some(config.getInt(path))
    } else {
      None
    }
  }

  def getOptionalString(path: String): Option[String] = {
    if (config.hasPath(path)) {
      Some(config.getString(path))
    } else {
      None
    }
  }

}
