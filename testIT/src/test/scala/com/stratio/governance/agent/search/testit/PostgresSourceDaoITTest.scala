package com.stratio.governance.agent.search.testit

import com.stratio.governance.agent.search.testit.utils.postgres.{PostgresTable, PostgresUtils}
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresSourceDao
import com.stratio.governance.agent.searcher.main.AppConf
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class PostgresSourceDaoITTest extends FlatSpec with BeforeAndAfterAll {
  var utils: PostgresUtils = PostgresUtils.NONE
  val database: String = "dg_database"

  override def beforeAll(): Unit = {
    utils = PostgresUtils.builder("dg_database", "PostgresSourceDaoITTest")
      .withTable(
        PostgresTable("data_asset")
          .withColumn("id","SERIAL")
          .withColumn("name","TEXT")
          .withColumn("description","TEXT")
          .withColumn("metadata_path","TEXT NOT NULL UNIQUE")
          .withColumn("type","TEXT NOT NULL")
          .withColumn("subtype","TEXT NOT NULL")
          .withColumn("properties","jsonb NOT NULL")
          .withColumn("active","BOOLEAN NOT NULL")
          .withColumn("discovered_at","TIMESTAMP NOT NULL")
          .withColumn("modified_at","TIMESTAMP NOT NULL")
          .withConstraint("pk_data_asset PRIMARY KEY (id)")
          .withConstraint("u_data_asset_meta_data_path_tenant UNIQUE (metadata_path, tenant)")
      )
      .withTable(
        PostgresTable("partial_indexation_state")
          .withColumn("id","SMALLINT NOT NULL UNIQUE")
          .withColumn("last_read_data_asset","TIMESTAMP")
          .withColumn("last_read_key_data_asset","TIMESTAMP")
          .withColumn("last_read_key","TIMESTAMP")
          .withColumn("last_read_business_assets_data_asset","TIMESTAMP")
          .withColumn("last_read_business_assets","TIMESTAMP")
          .withConstraint("pk_data_asset_last_ingested PRIMARY KEY (id)")
      )
      .withTable(
        PostgresTable("total_indexation_state")
          .withColumn("id","SMALLINT NOT NULL UNIQUE")
          .withColumn("last_read_data_asset","TIMESTAMP")
          .withConstraint("pk_data_asset_last_ingested PRIMARY KEY (id)")
      )
      .withTable(
        PostgresTable("key")
          .withColumn("id","SERIAL")
          .withColumn("key","TEXT NOT NULL")
          .withColumn("description","TEXT")
          .withColumn("status","BOOLEAN NOT NULL")
          .withColumn("tenant","TEXT NOT NULL")
          .withColumn("modified_at","TIMESTAMP NOT NULL")
          .withConstraint("pk_key PRIMARY KEY (id)")
          .withConstraint("u_key_key_tenant UNIQUE (key, tenant)")
      )
      .withTable(
        PostgresTable("key_data_asset")
          .withColumn("id","SERIAL")
          .withColumn("value","TEXT NOT NULL")
          .withColumn("key_id","INTEGER NOT NULL")
          .withColumn("data_asset_id","INTEGER NOT NULL")
          .withColumn("tenant","TEXT NOT NULL")
          .withColumn("modified_at","TIMESTAMP NOT NULL")
          .withConstraint("pk_key_value PRIMARY KEY (id)")
          .withConstraint("fk_key_value_key FOREIGN KEY (key_id) REFERENCES dg_metadata.key(id) ON DELETE CASCADE")
          .withConstraint("fk_key_value_data_asset FOREIGN KEY (data_asset_id) REFERENCES dg_metadata.data_asset(id) ON DELETE CASCADE")
      )
      .withTable(
        PostgresTable("community")
          .withColumn("id","SERIAL")
          .withColumn("name","TEXT NOT NULL")
          .withColumn("description","TEXT")
          .withColumn("tenant","TEXT NOT NULL")
          .withConstraint("pk_community PRIMARY KEY(id)")
          .withConstraint("u_community_name_tenant UNIQUE (name, tenant)")
      )
      .withTable(
        PostgresTable("domain")
          .withColumn("name","TEXT NOT NULL")
          .withColumn("description","TEXT")
          .withColumn("community_id","INTEGER NOT NULL")
          .withColumn("tenant","TEXT NOT NULL")
          .withConstraint("pk_domain PRIMARY KEY(id)")
          .withConstraint("u_domain_name_community_id UNIQUE (name, community_id)")
          .withConstraint("fk_domain_community FOREIGN KEY (community_id) REFERENCES dg_metadata.community(id) ON DELETE CASCADE")
      )
      .withTable(
        PostgresTable("business_assets_type")
          .withColumn("id","SERIAL")
          .withColumn("name","TEXT NOT NULL UNIQUE")
          .withColumn("description","TEXT")
          .withColumn("properties","jsonb")
          .withConstraint("pk_business_assets_type PRIMARY KEY(id)")
          .insert(Map("name"-> "'TERM'", "description" -> "'Business term'", "properties" -> "'{\"description\":\"string\",\"examples\":\"string\"}'"))
          //.insert(Map("name"-> "'QR'", "description" -> "'Quality rules'", "properties" -> "'{\"volumetria\":\"integer\",\"regex\":\"string\"}'"))
      )
      .withTable(
        PostgresTable("business_assets_status")
          .withColumn("id","SERIAL")
          .withColumn("name","TEXT NOT NULL")
          .withColumn("description","TEXT")
          .withConstraint("pk_business_assets_status PRIMARY KEY(id)")
          .insert(Map("description"-> "'Approved'", "name"-> "'APR'"))
          .insert(Map("description"-> "'Pending'", "name"-> "'PEN'"))
          .insert(Map("description"-> "'Under review'", "name"-> "'UNR'"))
      )
      .withTable(
        PostgresTable("business_assets")
          .withColumn("id","SERIAL")
          .withColumn("name","TEXT NOT NULL")
          .withColumn("description","TEXT")
          .withColumn("properties","jsonb")
          .withColumn("tenant","TEXT NOT NULL")
          .withColumn("business_assets_type_id","INTEGER NOT NULL")
          .withColumn("domain_id","INTEGER NOT NULL")
          .withColumn("business_assets_status_id","INTEGER NOT NULL")
          .withColumn("modified_at","TIMESTAMP NOT NULL")
          .withConstraint("pk_business_assets PRIMARY KEY(id)")
          .withConstraint("fk_business_assets_business_assets_type FOREIGN KEY (business_assets_type_id) REFERENCES dg_metadata.business_assets_type(id)")
          .withConstraint("fk_business_assets_domain FOREIGN KEY (domain_id) REFERENCES dg_metadata.domain(id) ON DELETE CASCADE")
          .withConstraint("fk_business_assets_business_assets_status FOREIGN KEY (business_assets_status_id) REFERENCES dg_metadata.business_assets_status(id)")
          .withConstraint("u_business_assets_name_domain_id UNIQUE (name, domain_id)")
      )
      .withTable(
        PostgresTable("business_assets_data_asset")
          .withColumn("id","SERIAL")
          .withColumn("name","TEXT NOT NULL")
          .withColumn("description","TEXT")
          .withColumn("tenant","TEXT NOT NULL")
          .withColumn("data_asset_id","INTEGER NOT NULL")
          .withColumn("business_assets_id","INTEGER NOT NULL")
          .withColumn("modified_at","TIMESTAMP NOT NULL")
          .withConstraint("pk_business_assets_data_asset PRIMARY KEY(id)")
          .withConstraint("fk_business_assets_business_assets_id_business_assets FOREIGN KEY (business_assets_id) REFERENCES dg_metadata.business_assets(id) ON DELETE CASCADE")
          .withConstraint("fk_business_assets_data_asset_id_data_asset FOREIGN KEY (data_asset_id) REFERENCES dg_metadata.data_asset(id) ON DELETE CASCADE")
          .withConstraint("u_business_assets_data_asset_data_asset_id_business_assets_id UNIQUE (data_asset_id, business_assets_id)")
      )
      .build.createAll
  }

  override def afterAll(): Unit = {
    utils.dropAll
  }

  "PostgresDao constructor " should " create all tables if is not " in {
    val database = "postgresDaoTest1"
    val schema : String = "schema"
    val exponentialBackOff :ExponentialBackOff = ExponentialBackOff(AppConf.extractorExponentialbackoffPauseMs, AppConf.extractorExponentialbackoffMaxErrorRetry)
    val postgresDao: PostgresSourceDao = new PostgresSourceDao(AppConf.sourceConnectionUrl, AppConf.sourceConnectionUser, AppConf.sourceConnectionPassword, database, schema, 1, 4, exponentialBackOff,true)


    
  }

}
