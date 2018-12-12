package com.stratio.governance.agent.search.testit.utils.postgres

import scala.collection.mutable.ListBuffer

case class PostgresTable(name: String, ifNotExist: Boolean = false) {
  var columns: Map[String, String] = Map()
  var constraints: ListBuffer[String] = ListBuffer()
  var inserts: ListBuffer[Map[String, String]] = ListBuffer()

  def withColumn(name: String, `type`: String, options: String = ""): PostgresTable = {
    columns += (name -> `type`.concat(" ").concat(options))
    this
  }

  def withConstraint(constraint: String): PostgresTable = {
    constraints +=  s"CONSTRAINT $constraint"
    this
  }

  def insert(value: Map[String, String]): PostgresTable = {
    val keysNotInColumns = value.keys.filter(!columns.keys.toList.contains(_))
    if (keysNotInColumns.nonEmpty) {
      throw new IllegalArgumentException("You try to insert over non-created columns: ".concat(keysNotInColumns.mkString(", ")))
    }
    inserts += value
    this
  }

  def generateInserts(schemaName: String): List[String] = {
    val ret: ListBuffer[String] = ListBuffer()
    inserts.foreach((map: Map[String, String])=>{
      val queryBuilder: StringBuilder = new StringBuilder()
        .append(s"INSERT INTO $schemaName.$name(")
      var names : String = ""
      var values: String = ""
      var prefix: String  = ""
      map.foreach { case (key: String, value: String) =>
        names = names + prefix + key
        values = values + prefix + value
        prefix = ", "
      }
      ret += queryBuilder.append(names).append(") VALUES (").append(values).append(")").mkString
    })
    ret.toList
  }
}
