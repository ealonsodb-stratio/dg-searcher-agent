package com.stratio.governance.agent.searcher.actors.dao

import java.sql.Timestamp
import java.time.Instant

import com.stratio.governance.commons.agent.domain.dao.DataAssetDao

object DataAssetDaoWrapper {


  def readDataAssetsSince(instant: Instant, limit: Long) : Tuple2[Array[DataAssetDao],Instant] = {
    Tuple2(Array(
      DataAssetDao(
        1,
        Some("fake_column"),
        Some("fake_description"),
        "fake_metadatapath",
        "fake_type",
        "fake_subtype",
        "fake_tenant",
        "fake_properties",
        false,
        Timestamp.from(Instant.now()),
        Timestamp.from(Instant.now())
      )
    ), Instant.now()) //TODO la fecha devuelta ya no es ahora sino la fecha del ultimo registro devuelto
  }

  def readLastUpdatedInstant(): Option[Instant] =  Some(Instant.MIN)

  def writeLastUpdatedInstant(instant: Instant): Unit = {

  }
}
