# Databricks notebook source
# MAGIC %md
# MAGIC #### Data Ingestion with AutoLoader
# MAGIC from S3(csv files) to Delta Tables with Autoloader(structured streaming) 
# MAGIC 1. directory listing with Incremental ingestion option -> multiple csv in single DataFrame
# MAGIC 2. simple data transformation
# MAGIC 3. writeStream

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### configuration 

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Input config

# COMMAND ----------

#import necessary package 
import os
import re
from delta.tables import *
from datetime import datetime

#import necessary package 
from pyspark.sql import functions as F 
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql import Window, Row 

#input_directory: folder need to monitor 
# input_directory = "dbfs:/mnt/uct-landing-gen-dev/AOI/inspection_result"
input_directory = "dbfs:/mnt/uct-landing-gen-dev/AOI/inspection_result_reformat"
#schema_location: used for storing the infered schema from readStream.format('csv')
schema_location = "dbfs:/mnt/uct-landing-gen-dev/AOI/schema_location"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### output config

# COMMAND ----------

#delta_path: for writing the delta table
delta_path = "dbfs:/mnt/uct-transform-gen-dev/AOI/aoi_inspection_result_streaming"

#checkpoint_path: for writing streamWrite checkpoints
ckpt_path = "dbfs:/mnt/uct-transform-gen-dev/AOI/aoi_streaming_ckpt"

#output mode: append, complete, update
output_mode = "append"


# COMMAND ----------

### remove and refresh the process 
# dbutils.fs.ls(schema_location)
# dbutils.fs.ls(delta_path)
# dbutils.fs.ls(ckpt_path)

# print(dbutils.fs.rm(schema_location, recurse=True))
# print(dbutils.fs.rm(delta_path, recurse=True))
# print(dbutils.fs.rm(ckpt_path, recurse=True))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Autoloader readStream

# COMMAND ----------

df_stream = spark.readStream.format('cloudFiles')\
            .option("cloudFiles.format", "csv")\
            .option("cloudFiles.schemaLocation", schema_location)\
            .option("cloudFiles.schemaEvolutionMode","none")\
            .option("cloudFiles.useIncrementalListing", "true")\
            .option("cloudFiles.backfillInterval", "1 week")\
            .option("cloudFiles.allowOverwrites", "true")\
            .load(input_directory)

# COMMAND ----------

display(df_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Preprocessing 
# MAGIC extract Model, inspection_date, Sales_order and refine the imagelink with pandas udf 

# COMMAND ----------

#string split function with udf 
dict_schema = StructType()\
    .add("image_link", StringType(), nullable=True)\
    .add("model", StringType(), nullable=True)\
    .add("sale_order", StringType(), nullable=True)\
    .add("inspection_date", StringType(), nullable=True)

@F.udf(returnType=dict_schema)
def extract_info(imglink):
    #case1: imglink=HYPERLINK("Q:\Inspection Results\MPD2\575-313788-CE5900A_13112022_1633\SO VERIFICATION.bmp")
    #case2: imglink=Q:\\Inspection Results\\ACDC\\575-341084-F0014_30082022_1728\\DoorPanelSO.bmp
    #need to validate of path 
    try:
        items = imglink.split('"')[1].split("\\")[-3:]
    except: 
        items = imglink.split("\\")[-3:]
    new_imglink = "/".join(items)
    model = items[0]
    sale_order, date, time = items[1].split("_")
    date = re.findall("\d+", date)[0]
    timestamp = datetime.strptime(f"{date}_{time}", "%d%m%Y_%H%M")\
        .strftime("%Y-%m-%d %H:%M")
    return {"image_link":new_imglink, \
            "model":model, \
            "sale_order":sale_order, \
            "inspection_date":timestamp}          

# COMMAND ----------

df_stream.columns

# COMMAND ----------

##call user defined method and rename all the columns
df_stream_addInfo = \
    df_stream.select( \
         col("Recipe Location").alias("recipe_location"),\
         col("Recipe").alias("recipe"),\
         col("Name").alias("name"),\
         col("Program").alias("program"),\
         col("InspectedResult").alias("inspection_result"),\
         col("ExpctedResult").alias("expected_result"),\
         col("ComponentName").alias("component_name"),\
         extract_info(df_stream["ImageLink"]).image_link.alias("image_link"),\
         extract_info(df_stream["ImageLink"]).sale_order.alias("sale_order"),\
         extract_info(df_stream["ImageLink"]).model.alias("model"),\
         F.to_timestamp(\
                   extract_info(df_stream["ImageLink"]).inspection_date)\
                   .alias("inspection_date"),\
         col("Pass/Fail").alias("pass_fail"),\
         col("Overwrite").alias("overwrite"),\
         col("Overall").alias("overall"))

df_stream_addInfo = df_stream_addInfo\
    .withColumn("ingestion_date",  F.current_timestamp())\
    .withColumn('data_source', lit('AOI'))

# COMMAND ----------

display(df_stream_addInfo.printSchema())

# COMMAND ----------

#HOW to change nullability of schema while streaming from false to true for example
## reference : https://stackoverflow.com/questions/33193958/change-nullable-property-of-column-in-spark-dataframe

df_stream_addInfo = df_stream_addInfo\
    .withColumn("ingestion_date",  F.when(col("ingestion_date").isNotNull(), col("ingestion_date")).otherwise(F.lit(None)))\
    .withColumn('data_source', F.when(col("data_source").isNotNull(), col("data_source")).otherwise(F.lit(None)))

# COMMAND ----------

display(df_stream_addInfo.groupBy("data_source").count())

# COMMAND ----------

###extract the overall and inspectation date columns 
###and rejoin them with orginal dataframes to get the time_take column
two_cols = df_stream_addInfo.select(\
               F.unix_timestamp("overall","HH:mm:ss.SS").alias("time_taken"),\
               col("inspection_date").alias("i_date"),\
                 col("ingestion_date").alias("in_date"))\
                .where(df_stream_addInfo["overall"].rlike('[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}'))
##inner join can result in data missing if the overall time in the original stream is null value 
# df_stream_addInfo_1 = df_stream_addInfo.join(two_cols,\
#                                 two_cols.i_date == df_stream_addInfo.inspection_date,\
#                                 "inner").drop("overall", "i_date")

## apply left join with watermark(eventTime, "threshold of delay")
# two_cols_withWaterMark = two_cols.withWatermark("in_date", "1 minute")
# df_stream_addInfo_withWaterMark = df_stream_addInfo.withWatermark("ingestion_date", "1 minute")

# df_stream_addInfo_1 = df_stream_addInfo_withWaterMark.join(two_cols_withWaterMark, \
#                                                            F.expr("""
#                                                            inspection_date = i_date AND 
#                                                            ingestion_date = in_date 
#                                                            """),\
#                  "left").drop("overall", "in_date")
two_cols_withWaterMark = two_cols.withWatermark("in_date", "1 minute")

df_stream_addInfo_1 = df_stream_addInfo.join(two_cols_withWaterMark, \
                                                           F.expr("""
                                                           inspection_date = i_date AND 
                                                           ingestion_date = in_date 
                                                           """),\
                 "left").drop("overall", "in_date")

# COMMAND ----------

display(two_cols_withWaterMark)

# COMMAND ----------

display(df_stream_addInfo_1)

# COMMAND ----------

df_stream_addInfo_1.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Autoloader WriteStream

# COMMAND ----------

 #Trigger option: processingTime="2 seconds; 2 minutes; 2 days"; once=True; availableNow;continuous='1 second'
# if no explicitly specified, then , the query will be executed in micro-batch mode, where micro-batches will be 
# generated as soon as the previous micro-batch has completed processing.
query = df_stream_addInfo_1.writeStream.format("delta").\
    queryName("AOI_streaming_ingestion").\
    option("checkpointLocation", ckpt_path).\
    start(delta_path)
query.awaitTermination()

# COMMAND ----------

# dbutils.fs.rm(delta_path, recurse=True)
test_df = spark.read.load(delta_path)
display(test_df)

# COMMAND ----------

display(test_df.groupBy("inspection_date").count())
