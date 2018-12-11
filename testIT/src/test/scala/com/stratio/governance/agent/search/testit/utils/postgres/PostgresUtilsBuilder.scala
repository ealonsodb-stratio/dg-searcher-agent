package com.stratio.governance.agent.search.testit.utils.postgres

import scala.collection.mutable.ListBuffer

case class PostgresUtilsBuilder(database: String, schema: String) {
  var tables : ListBuffer[PostgresTable] = ListBuffer()

  def withTable(table: PostgresTable): PostgresUtilsBuilder = {
    this.tables += table
    this
  }

  def build: PostgresUtils = PostgresUtils(database, schema, tables.toList)
}
