package com.stratio.governance.agent.search.testit.utils.postgres

import java.sql.ResultSet

case class PostgresUtils(databaseName: String, schemaName: String, tables: List[PostgresTable]) {
  val tables_by_name: Map[String, PostgresTable] = tables.map(table => table.name -> table).toMap

  val connection: PostgresConnection= new PostgresConnection()

  def createDatabase: PostgresUtils = {
    if (!execute(s"SELECT EXISTS(SELECT 1 from pg_database WHERE datname='$databaseName')").getBoolean("exists"))
      execute(s"CREATE DATABASE $databaseName")
    execute(s"\\c $databaseName")
    this
  }

  def dropDatabase: PostgresUtils = {
    execute(s"DROP DATABASE $databaseName CASCADE")
    this
  }

  def createSchema: PostgresUtils = {
    execute(s"CREATE SCHEMA IF NOT EXISTS $schemaName")
    this
  }

  def dropSchema: PostgresUtils = {
    execute(s"DROP SCHEMA $schemaName CASCADE")
    this
  }

  def createAndInsertAll: PostgresUtils = createDatabase.createSchema.createTablesAndInsert

  def dropAll: PostgresUtils = dropDatabase

  def createTables: PostgresUtils = {
    tables.foreach((table: PostgresTable) => {
      val queryBuilder = new StringBuilder().append("CREATE TABLE ")
      if (table.ifNotExist) queryBuilder.append("IF NOT EXISTS ")
      queryBuilder.append(s"$schemaName.${table.name} (")
      var prefix = ""
      table.columns.keys.foreach((key: String) => {
        queryBuilder.append(prefix.concat(key).concat(" ").concat(table.columns(key)))
        prefix = ", "
      })
      table.constraints.foreach((cons: String) => queryBuilder.append(prefix.concat(cons)))
      queryBuilder.append(")")
      execute(queryBuilder.mkString)
    })
    this
  }

  def insertIntoTables(): PostgresUtils = {
    tables.foreach((table: PostgresTable)=> {
      table.generateInserts(schemaName).foreach(execute)
    })
    this
  }

  def createTablesAndInsert: PostgresUtils = {
    createTables.insertIntoTables()
  }

  def dropTables: PostgresUtils = {
    tables.foreach((table: PostgresTable)=> {
      execute(s"DROP TABLE $schemaName.${table.name} CASCADE")
    })
    this
  }

  def execute (query: String): ResultSet = {
    connection.execute(query)
  }

  def getTable(name: String): PostgresTable =
    tables_by_name(name)

}

object PostgresUtils {
  def builder(databaseName: String, schemaName: String): PostgresUtilsBuilder = PostgresUtilsBuilder(databaseName, schemaName)
  val NONE: PostgresUtils = PostgresUtils("none","none", List())
}