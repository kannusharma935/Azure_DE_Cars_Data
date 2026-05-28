# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

df=spark.read.format('parquet')\
        .option('inferSchema',True)\
        .load('abfss://bronze@azuredwhengglake.dfs.core.windows.net/rawdata')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DAta Tranformation
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df=df.withColumn('model_category',split(col('Model_ID'),'-')[0]) 
df.display()

# COMMAND ----------

df=df.withColumn('RevPerUnit',col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ad-hoc
# MAGIC

# COMMAND ----------

display(df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Total_Units')).sort('Year','Total_Units',ascending=[1,0]))


# COMMAND ----------

# MAGIC %md
# MAGIC ## data writing to silver
# MAGIC

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
    .option('path','abfss://silver@azuredwhengglake.dfs.core.windows.net/Carsales')\
        .save()    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * From parquet.`abfss://silver@azuredwhengglake.dfs.core.windows.net/Carsales`

# COMMAND ----------

