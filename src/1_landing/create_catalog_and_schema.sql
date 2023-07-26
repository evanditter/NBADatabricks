-- Databricks notebook source
-- CREATE CATALOG IF EXISTS NBA

-- COMMAND ----------

CREATE EXTERNAL LOCATION test-NBA
URL 's3://databricks-workspace-stack-7be16-metastore-bucket'
WITH (STORAGE CREDENTIAL `metastore-us-east-2`);

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS NBA.raw

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS NBA.analytics
