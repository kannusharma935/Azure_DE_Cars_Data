# Databricks notebook source
# MAGIC %md
# MAGIC # Create Catalog 

# COMMAND ----------

# MAGIC %sql
# MAGIC Create catalog cars_catalog;
# MAGIC --catalog = databse is synapse

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.silver;
# MAGIC --schema = table is synapse

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.gold;

# COMMAND ----------

