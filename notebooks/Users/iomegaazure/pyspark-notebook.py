# Databricks notebook source
salesDF = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/salesdata/FactSales*.csv")
  
salesDF.write.mode("append").parquet("/mnt/salesdata/parquet/sales")

salesDF.printSchema

# COMMAND ----------

import os.path
import IPython
from pyspark.sql import SQLContext

# COMMAND ----------

dbutils.fs.put("x "Hello, World!", True)
dbutils.fs.ls("/mnt/salesdata/parquet/sales")

# COMMAND ----------

salesDF = spark.read.format('parquet').options(header='true', inferschema='true').load("/mnt/salesdata/parquet/sales")
salesDF.printSchema
salesDF.show(20, False)

# COMMAND ----------

display(salesDF)

# COMMAND ----------

sales = spark.read.parquet('/mnt/salesdata/parquet/sales')
sales.createOrReplaceTempView('sales')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales WHERE StoreId BETWEEN 1 and 10