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
          .insert(Map( "id" -> "192", "name" -> "R_REGIONKEY", "description" -> "Hdfs parquet column", "metadata_path" -> "hdfsFinance://department/finance/'2018>/:region.parquet:R_REGIONKEY:", "type" -> "HDFS", "subtype" -> "FIELD", "tenant" -> "NONE", "properties" -> "'{\"type\": \"long\", \"default\": \"\", \"constraint\": \"\", \"schemaType\": \"parquet\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "193", "name" -> "region.parquet", "description" -> "Hdfs file", "metadata_path" -> "hdfsFinance://department/marketing/2017>/:region.parquet:", "type" -> "HDFS", "subtype" -> "RESOURCE", "tenant" -> "NONE", "properties" -> "'{\"group\": \"supergroup\", \"owner\": \"hdfs\", \"length\": 455, \"schema\": \"na\", \"blockSize\": 134217728, \"modifiedAt\": 1544433863730, \"isEncrypted\": \"false\", \"permissions\": \"644\", \"replication\": 3}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "194", "name" -> "finance", "description" -> "finance Hdfs directory", "metadata_path" -> "hdfsFinance://department>/finance:", "type" -> "HDFS", "subtype" -> "PATH", "tenant" -> "NONE", "properties" -> "'{\"group\": \"supergroup\", \"owner\": \"hdfs\", \"isEncrypted\": \"false\", \"permissions\": \"755\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "195", "name" -> "R_REGIONKEY", "description" -> "Hdfs parquet column", "metadata_path" -> "hdfsFinance://department/finance/2017>/:region.parquet:R_REGIONKEY:", "type" -> "HDFS", "subtype" -> "FIELD", "tenant" -> "NONE", "properties" -> "'{\"type\": \"long\", \"default\": \"\", \"constraint\": \"\", \"schemaType\": \"parquet\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "196", "name" -> "R_COMMENT", "description" -> "Hdfs parquet column", "metadata_path" -> "hdfsFinance://department/marketing/'2018>/:region.parquet:R_COMMENT:", "type" -> "HDFS", "subtype" -> "FIELD", "tenant" -> "NONE", "properties" -> "'{\"type\": \"org.apache.parquet.io.api.Binary\", \"default\": \"\", \"constraint\": \"\", \"schemaType\": \"parquet\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "197", "name" -> "hdfsFinance", "description" -> "Hdfs datastore", "metadata_path" -> "hdfsFinance:", "type" -> "HDFS", "subtype" -> "DS", "tenant" -> "NONE", "properties" -> "'{\"url\": \"url\", \"version\": \"1.0.0\", \"security\": \"TLS\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "198", "name" -> "department", "description" -> "Hdfs directory", "metadata_path" -> "hdfsFinance:/>/department:", "type" -> "HDFS", "subtype" -> "PATH", "tenant" -> "NONE", "properties" -> "'{\"group\": \"supergroup\", \"owner\": \"hdfs\", \"isEncrypted\": \"false\", \"permissions\": \"755\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "199", "name" -> "'2018", "description" -> "Hdfs directory", "metadata_path" -> "hdfsFinance://department/finance>/'2018:", "type" -> "HDFS", "subtype" -> "PATH", "tenant" -> "NONE", "properties" -> "'{\"group\": \"supergroup\", \"owner\": \"hdfs\", \"isEncrypted\": \"false\", \"permissions\": \"755\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "200", "name" -> "2017", "description" -> "Hdfs directory", "metadata_path" -> "hdfsFinance://department/finance>/2017:", "type" -> "HDFS", "subtype" -> "PATH", "tenant" -> "NONE", "properties" -> "'{\"group\": \"supergroup\", \"owner\": \"hdfs\", \"isEncrypted\": \"false\", \"permissions\": \"755\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "201", "name" -> "R_REGIONKEY", "description" -> "Hdfs parquet column", "metadata_path" -> "hdfsFinance://department/marketing/'2018>/:region.parquet:R_REGIONKEY:", "type" -> "HDFS", "subtype" -> "FIELD", "tenant" -> "NONE", "properties" -> "'{\"type\": \"long\", \"default\": \"\", \"constraint\": \"\", \"schemaType\": \"parquet\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "202", "name" -> "R_REGIONKEY", "description" -> "Hdfs parquet column", "metadata_path" -> "hdfsFinance://department/marketing/2017>/:region.parquet:R_REGIONKEY:", "type" -> "HDFS", "subtype" -> "FIELD", "tenant" -> "NONE", "properties" -> "'{\"type\": \"long\", \"default\": \"\", \"constraint\": \"\", \"schemaType\": \"parquet\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "203", "name" -> "R_COMMENT", "description" -> "Hdfs parquet column", "metadata_path" -> "hdfsFinance://department/finance/'2018>/:region.parquet:R_COMMENT:", "type" -> "HDFS", "subtype" -> "FIELD", "tenant" -> "NONE", "properties" -> "'{\"type\": \"org.apache.parquet.io.api.Binary\", \"default\": \"\", \"constraint\": \"\", \"schemaType\": \"parquet\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "204", "name" -> "marketing", "description" -> "Hdfs directory", "metadata_path" -> "hdfsFinance://department>/marketing:", "type" -> "HDFS", "subtype" -> "PATH", "tenant" -> "NONE", "properties" -> "'{\"group\": \"supergroup\", \"owner\": \"hdfs\", \"isEncrypted\": \"false\", \"permissions\": \"755\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "205", "name" -> "R_COMMENT", "description" -> "Hdfs parquet column", "metadata_path" -> "hdfsFinance://department/marketing/2017>/:region.parquet:R_COMMENT:", "type" -> "HDFS", "subtype" -> "FIELD", "tenant" -> "NONE", "properties" -> "'{\"type\": \"org.apache.parquet.io.api.Binary\", \"default\": \"\", \"constraint\": \"\", \"schemaType\": \"parquet\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "206", "name" -> "region.parquet", "description" -> "Hdfs file", "metadata_path" -> "hdfsFinance://department/marketing/'2018>/:region.parquet:", "type" -> "HDFS", "subtype" -> "RESOURCE", "tenant" -> "NONE", "properties" -> "'{\"group\": \"supergroup\", \"owner\": \"hdfs\", \"length\": 455, \"schema\": \"na\", \"blockSize\": 134217728, \"modifiedAt\": 1544433868101, \"isEncrypted\": \"false\", \"permissions\": \"644\", \"replication\": 3}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "207", "name" -> "R_NAME", "description" -> "Hdfs parquet column", "metadata_path" -> "hdfsFinance://department/marketing/2017>/:region.parquet:R_NAME:", "type" -> "HDFS", "subtype" -> "FIELD", "tenant" -> "NONE", "properties" -> "'{\"type\": \"org.apache.parquet.io.api.Binary\", \"default\": \"\", \"constraint\": \"\", \"schemaType\": \"parquet\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "208", "name" -> "R_NAME", "description" -> "Hdfs parquet column", "metadata_path" -> "hdfsFinance://department/finance/'2018>/:region.parquet:R_NAME:", "type" -> "HDFS", "subtype" -> "FIELD", "tenant" -> "NONE", "properties" -> "'{\"type\": \"org.apache.parquet.io.api.Binary\", \"default\": \"\", \"constraint\": \"\", \"schemaType\": \"parquet\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "209", "name" -> "R_NAME", "description" -> "Hdfs parquet column", "metadata_path" -> "hdfsFinance://department/finance/2017>/:region.parquet:R_NAME:", "type" -> "HDFS", "subtype" -> "FIELD", "tenant" -> "NONE", "properties" -> "'{\"type\": \"org.apache.parquet.io.api.Binary\", \"default\": \"\", \"constraint\": \"\", \"schemaType\": \"parquet\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "210", "name" -> "R_NAME", "description" -> "Hdfs parquet column", "metadata_path" -> "hdfsFinance://department/marketing/'2018>/:region.parquet:R_NAME:", "type" -> "HDFS", "subtype" -> "FIELD", "tenant" -> "NONE", "properties" -> "'{\"type\": \"org.apache.parquet.io.api.Binary\", \"default\": \"\", \"constraint\": \"\", \"schemaType\": \"parquet\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "211", "name" -> "region.parquet", "description" -> "Hdfs file", "metadata_path" -> "hdfsFinance://department/finance/2017>/:region.parquet:", "type" -> "HDFS", "subtype" -> "RESOURCE", "tenant" -> "NONE", "properties" -> "'{\"group\": \"supergroup\", \"owner\": \"hdfs\", \"length\": 455, \"schema\": \"na\", \"blockSize\": 134217728, \"modifiedAt\": 1544433853808, \"isEncrypted\": \"false\", \"permissions\": \"644\", \"replication\": 3}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "212", "name" -> "'2018", "description" -> " Hdfs directory", "metadata_path" -> "hdfsFinance://department/marketing>/'2018:", "type" -> "HDFS", "subtype" -> "PATH", "tenant" -> "NONE", "properties" -> "'{\"group\": \"supergroup\", \"owner\": \"hdfs\", \"isEncrypted\": \"false\", \"permissions\": \"755\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "213", "name" -> "region.parquet", "description" -> "Hdfs file", "metadata_path" -> "hdfsFinance://department/finance/'2018>/:region.parquet:", "type" -> "HDFS", "subtype" -> "RESOURCE", "tenant" -> "NONE", "properties" -> "'{\"group\": \"supergroup\", \"owner\": \"hdfs\", \"length\": 455, \"schema\": \"na\", \"blockSize\": 134217728, \"modifiedAt\": 1544433849615, \"isEncrypted\": \"false\", \"permissions\": \"644\", \"replication\": 3}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "214", "name" -> "2017", "description" -> "Hdfs directory", "metadata_path" -> "hdfsFinance://department/marketing>/2017:", "type" -> "HDFS", "subtype" -> "PATH", "tenant" -> "NONE", "properties" -> "'{\"group\": \"supergroup\", \"owner\": \"hdfs\", \"isEncrypted\": \"false\", \"permissions\": \"755\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "215", "name" -> "R_COMMENT", "description" -> "Hdfs parquet column", "metadata_path" -> "hdfsFinance://department/finance/2017>/:region.parquet:R_COMMENT:", "type" -> "HDFS", "subtype" -> "FIELD", "tenant" -> "NONE", "properties" -> "'{\"type\": \"org.apache.parquet.io.api.Binary\", \"default\": \"\", \"constraint\": \"\", \"schemaType\": \"parquet\"}'", "active" -> "true", "discovered_at" -> "'2018-12-10T08:27:17Z'", "modified_at" -> "'2018-12-10T08:27:17Z'"))
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
          .insert(Map( "id" -> "1", "key" -> "OWNER", "description" -> "Owner", "status" -> "true", "tenant" -> "NONE", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "2", "key" -> "QUALITY", "description" -> "Quality", "status" -> "true", "tenant" -> "NONE", "modified_at" -> "'2018-12-10T08:27:17Z'"))
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
          .insert(Map( "id" -> "2", "value" -> "finance", "key_id" -> "1", "data_asset_id" -> "201", "tenant" -> "NONE", "modified_at" -> "'2018-12-10T08:27:17Z'"))
      )
      .withTable(
        PostgresTable("community")
          .withColumn("id","SERIAL")
          .withColumn("name","TEXT NOT NULL")
          .withColumn("description","TEXT")
          .withColumn("tenant","TEXT NOT NULL")
          .withConstraint("pk_community PRIMARY KEY(id)")
          .withConstraint("u_community_name_tenant UNIQUE (name, tenant)")
          .insert(Map( "id" -> "1", "name" -> "Marketing", "description" -> "Description marketing", "tenant" -> "NONE"))
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
          .insert(Map( "id" -> "1", "name" -> "Marketing domain", "description" -> "Description marketing domain", "community_id" -> "1", "tenant" -> "NONE"))
      )
      .withTable(
        PostgresTable("business_assets_type")
          .withColumn("id","SERIAL")
          .withColumn("name","TEXT NOT NULL UNIQUE")
          .withColumn("description","TEXT")
          .withColumn("properties","jsonb")
          .withConstraint("pk_business_assets_type PRIMARY KEY(id)")
          .insert(Map("name" -> "'TERM'", "description" -> "'Business term'", "properties" -> "'{\"description\":\"string\",\"examples\":\"string\"}'"))
          //.insert(Map("name"-> "'QR'", "description" -> "'Quality rules'", "properties" -> "'{\"volumetria\":\"integer\",\"regex\":\"string\"}'"))
      )
      .withTable(
        PostgresTable("business_assets_status")
          .withColumn("id","SERIAL")
          .withColumn("name","TEXT NOT NULL")
          .withColumn("description","TEXT")
          .withConstraint("pk_business_assets_status PRIMARY KEY(id)")
          .insert(Map("description" -> "'Approved'", "name" -> "'APR'"))
          .insert(Map("description" -> "'Pending'", "name" -> "'PEN'"))
          .insert(Map("description" -> "'Under review'", "name" -> "'UNR'"))
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
          .insert(Map( "id" -> "1", "name" -> "Production", "description" -> "desc bt1", "properties" -> "'{}'", "tenant" -> "NONE", "business_assets_type_id" -> "1", "domain_id" -> "1", "business_assets_status_id" -> "1", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "2", "name" -> "Client", "description" -> "desc bt1", "properties" -> "'{}'", "tenant" -> "NONE", "business_assets_type_id" -> "1", "domain_id" -> "1", "business_assets_status_id" -> "1", "modified_at" -> "'2018-12-10T08:27:17Z'"))
          .insert(Map( "id" -> "3", "name" -> "Department", "description" -> "desc bt1", "properties" -> "'{}'", "tenant" -> "NONE", "business_assets_type_id" -> "1", "domain_id" -> "1", "business_assets_status_id" -> "1", "modified_at" -> "'2018-12-10T08:27:17Z'"))
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
          .insert(Map( "id" -> "1", "name" -> "", "description" -> "", "tenant" -> "NONE", "data_asset_id" -> "201", "business_assets_id" -> "1", "modified_at" -> "'2018-12-10T08:27:17Z'"))
      )
      .build.createAndInsertAll
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
