package com.stratio.governance.agent.search.testit.utils.postgres

import java.sql.{Connection, ResultSet}

import com.stratio.governance.agent.searcher.main.AppConf
import org.apache.commons.dbcp.PoolingDataSource
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

class PostgresConnection {

  // initialize JDBC driver & connection pool
  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton(AppConf.sourceConnectionUrl, AppConf.sourceConnectionUser, AppConf.sourceConnectionPassword, ConnectionPoolSettings(AppConf.sourceConnectionInitialSize, AppConf.sourceConnectionMaxSize))
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].setAccessToUnderlyingConnectionAllowed(true)
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].getConnection().setAutoCommit(true)

  var connection: Connection = ConnectionPool.borrow()

  def execute(query: String): ResultSet = {
    connection.createStatement().executeQuery(query)
  }
}
