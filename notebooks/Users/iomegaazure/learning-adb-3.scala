// Databricks notebook source
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

// COMMAND ----------

val config = Config(Map(
  "url"            -> "iomegasqlserverv2.database.windows.net",
  "databaseName"   -> "iomegasqldatabasev2",
  "dbTable"        -> "dbo.Clients",
  "user"           -> "iomegaadmin",
  "password"       -> "Prestige123",
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

// COMMAND ----------

val collection = spark.read.sqlDB(config)

collection.printSchema

// COMMAND ----------

display(collection)

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

// COMMAND ----------

var bulkCopyMetadata = new BulkCopyMetadata
bulkCopyMetadata.addColumnMetadata(1, "Title", java.sql.Types.NVARCHAR, 10, 0)
bulkCopyMetadata.addColumnMetadata(2, "FirstName", java.sql.Types.NVARCHAR, 100, 0)
bulkCopyMetadata.addColumnMetadata(3, "LastName", java.sql.Types.NVARCHAR, 100, 0)

// COMMAND ----------

val bulkCopyConfig = Config(Map(
  "url"               -> "iomegasqlserverv2.database.windows.net",
  "databaseName"      -> "iomegasqldatabasev2",
  "user"              -> "iomegaadmin",
  "password"          -> "Prestige123",
  "dbTable"           -> "dbo.CustomerNames",
  "bulkCopyBatchSize" -> "2500",
  "bulkCopyTableLock" -> "true",
  "bulkCopyTimeout"   -> "600"
))

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val customerSchema = List(
  StructField("Title", StringType, true),
  StructField("FirstName", StringType, true),
  StructField("LastName", StringType, true)
)
val data = sc.textFile("dbfs:///mnt/salesdata/customer-names.csv").
  mapPartitionsWithIndex((index, iterator) => {
    if(index == 0) iterator.drop(1)
    
    iterator
  }).map(line => {
    val splitted = line.split(",")
    
    Row(splitted(0), splitted(1), splitted(2))
  })

val customersFrame = spark.createDataFrame(data, StructType(customerSchema))

// COMMAND ----------

display(customersFrame)

// COMMAND ----------

customersFrame.bulkCopyToSqlDB(bulkCopyConfig, bulkCopyMetadata)