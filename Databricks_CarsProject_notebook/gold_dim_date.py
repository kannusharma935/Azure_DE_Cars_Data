# Databricks notebook source
# MAGIC %md
# MAGIC ## Create FLAG PARAMETER
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag=dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREATING DIMENSION MODEL
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Relative column
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(Date_ID) as Date_ID  From parquet.`abfss://silver@azuredwhengglake.dfs.core.windows.net/Carsales`

# COMMAND ----------

df_src=spark.sql('''select distinct(Date_ID) as Date_ID From parquet.`abfss://silver@azuredwhengglake.dfs.core.windows.net/Carsales`''')


# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_branch Sink- Initial and Incremental
# MAGIC #### Just bring schema if tablenot exists 

# COMMAND ----------


if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    df_sink=spark.sql('''select  dim_date_key,Date_ID
    From cars_catalog.gold.dim_date''')

else :
    df_sink=spark.sql('''select 1 as dim_date_key,Date_ID
    From parquet.`abfss://silver@azuredwhengglake.dfs.core.windows.net/Carsales`
    where 1=0''') 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering new records and old records
# MAGIC

# COMMAND ----------

df_filter=df_src.join(df_sink,df_src['Date_ID']==df_sink['Date_ID'],'left')\
    .select(df_src['Date_ID'],df_sink['dim_date_key'])


# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_old**

# COMMAND ----------

df_filter_old=df_filter.filter(col('dim_date_key').isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ***df filter new***
# MAGIC

# COMMAND ----------

df_filter_new=df_filter.filter(col('dim_date_key').isNull()).select(df_src['Date_ID'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### create surrogate Key
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch max surrogate key from exsisting table

# COMMAND ----------

if (incremental_flag=='0'):
    max_value=1
else :
    max_value_df =spark.sql("select max(dim_date_key) from cars_catalog.gold.dim_date")    
    max_value=max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Surrogate key column and ADD the max surrogate key**

# COMMAND ----------

df_filter_new=df_filter_new.withColumn('dim_date_key',max_value+monotonically_increasing_id())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create final DF- df_filter_old+df_filter_new

# COMMAND ----------

df_final=df_filter_new.union(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD Type -1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
     delta_tbl=DeltaTable.forPath(spark,"abfss://gold@azuredwhengglake.dfs.core.windows.net/dim_date")
     delta_tbl.alias("trg").merge(df_final.alias("src"),"trg.dim_date_key=src.dim_date_key")\
         .whenMatchedUpdateAll()\
         .whenNotMatchedInsertAll()\
         .execute()

# Initial run
else:
    df_final.write.format("delta")\
        .mode('overwrite')\
         .option("path","abfss://gold@azuredwhengglake.dfs.core.windows.net/dim_date")\
         .saveAsTable("cars_catalog.gold.dim_date")       


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_date;