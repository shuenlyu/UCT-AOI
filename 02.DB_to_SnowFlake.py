# Databricks notebook source
## table config
uct_transform_gen = spark.sql("select variable_value from Config.config_constant where variable_name = 'uct-transform-gen'").first()[0]

table_name = "aoi_inspection_result_batch"
write_format = "delta"
location = "AOI"
data_source = "AOI" #specify the name of data source, eg. AOI, AOI_1? 
write_path = f"/mnt/{uct_transform_gen}/{location}/{table_name}"

# COMMAND ----------

# load data from db delta table
aoi_df = spark.read.load(write_path)
display(aoi_df.groupBy("sale_order","inspection_date").count())


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write to Snowflake

# COMMAND ----------

sfUrl = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfUrl'").first()[0]
sfUser = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfUser'").first()[0]
sfPassword = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfPassword'").first()[0]
sfDatabase = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfDatabase'").first()[0]
sfSchema = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfSchema'").first()[0]
sfWarehouse = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfWarehouse'").first()[0]
sfRole = spark.sql("select variable_value from Config.config_constant where variable_name = 'sfRole'").first()[0]
insecureMode = spark.sql("select variable_value from Config.config_constant where variable_name = 'insecureMode'").first()[0]

options = {
  "sfUrl": sfUrl,
  "sfUser": "parthib.r@uct.com",
  "sfPassword": "pooParjas123",
  "sfDatabase": "MES_DEV",
  "sfSchema": "MES_EDW",
  "sfWarehouse": "UCT_DEV",
  "sfRole": sfRole,
  "insecureMode": insecureMode
}

# COMMAND ----------

aoi_df.write \
  .format("net.snowflake.spark.snowflake") \
  .options(**options) \
  .option("dbtable", "aoi_inspection_result_batch") \
  .mode("OVERWRITE") \
  .save()
