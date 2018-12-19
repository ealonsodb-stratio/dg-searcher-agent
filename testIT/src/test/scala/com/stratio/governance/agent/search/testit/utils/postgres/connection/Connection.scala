package com.stratio.governance.agent.search.testit.utils.postgres.connection

import java.sql.{ResultSet, Connection => SQLConnection}

import com.stratio.governance.agent.searcher.main.AppConf
import org.apache.commons.dbcp.PoolingDataSource
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

case class Connection(url: String, user: String, pass: String, initialSize: Int, maxSize: Int) {
  // initialize JDBC driver & connection pool
  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton(url, user, pass, ConnectionPoolSettings(initialSize, maxSize))
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].setAccessToUnderlyingConnectionAllowed(true)
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].getConnection().setAutoCommit(true)

  var connection: SQLConnection = ConnectionPool.borrow()

  def execute(query: String): ResultSet = {
    System.out.println(s"$this execute[$query]")
    connection.createStatement().executeQuery(query)
  }

  def executeUpdate(query: String): Int = {
    System.out.println(s"$this executeUpdate[$query]")
    connection.createStatement().executeUpdate(query)
  }
}

object Connection {
  val defaultConnection: Connection = Connection(AppConf.sourceConnectionUrl, AppConf.sourceConnectionUser, AppConf.sourceConnectionPassword, AppConf.sourceConnectionInitialSize, AppConf.sourceConnectionMaxSize)
}