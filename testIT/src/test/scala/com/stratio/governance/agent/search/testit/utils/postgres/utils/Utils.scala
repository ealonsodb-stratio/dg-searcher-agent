package com.stratio.governance.agent.search.testit.utils.postgres.utils

import com.stratio.governance.agent.search.testit.utils.postgres.PostgresSpace.Database
import com.stratio.governance.agent.search.testit.utils.postgres.builder.AbstractBuilder

case class Utils(database: Database) {
}

object Utils {
  val NONE: Utils = Utils(Database.builder("NONE").build)
  def builder(database: Database): UtilsBuilder = UtilsBuilder(database)

  case class UtilsBuilder(database: Database) extends AbstractBuilder[Utils]{

    def build: Utils = Utils(database)
  }

}