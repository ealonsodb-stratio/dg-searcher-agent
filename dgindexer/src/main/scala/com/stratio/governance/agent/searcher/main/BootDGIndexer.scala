package com.stratio.governance.agent.searcher.main

import com.stratio.governance.agent.searcher.actors.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.extractor.{DGExtractor, DGExtractorParams}
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexerParams
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer
import org.apache.commons.dbcp.PoolingDataSource
import scalikejdbc._

object BootDGIndexer extends App {

  // initialize JDBC driver & connection pool
  Class.forName("org.postgresql.Driver")
  ConnectionPoolSettings.apply(initialSize = 1000, maxSize = 1000)
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/hakama", "postgres", "######", ConnectionPoolSettings.apply(initialSize = 1000, maxSize = 1000))
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].setAccessToUnderlyingConnectionAllowed(true)
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].getConnection().setAutoCommit(false)

  // Initialize indexer params objects
  val dgIndexerParams: DGIndexerParams = new DGIndexerParams()
  val dgExtractorParams: DGExtractorParams = new DGExtractorParams {}

  // initialize the actor system
  val actorSystem: SearcherActorSystem[DGExtractor, DGIndexer] = new SearcherActorSystem[DGExtractor, DGIndexer]("dgIndexer", classOf[DGExtractor], classOf[DGIndexer], dgExtractorParams, dgIndexerParams)
  actorSystem.initPartialIndexation()
}
