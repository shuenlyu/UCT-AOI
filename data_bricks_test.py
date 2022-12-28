# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks trial

# COMMAND ----------

# MAGIC %md
# MAGIC #### Miscellanous

# COMMAND ----------

##python library test
%pip list 

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

file_location = "dbfs:/mnt/uct-landing-gen-dev/AOI/inspection_result_reformat/"
dbutils.fs.ls(file_location)
# print(dbutils.fs.rm(file_location, recurse=True))

# COMMAND ----------

# MAGIC %md
# MAGIC #### AutoLoader

# COMMAND ----------

import random
def random_checkpoint_dir(): 
  return "/tmp/chkpt/%s" % str(random.randint(0, 10000))

# COMMAND ----------

input_path = "dbfs:/mnt/uct-landing-gen-dev/AOI/inspection_result"
schema_location = "dbfs:/mnt/uct-landing-gen-dev/AOI/schema_location"
delta_path = "/tmp/test_db"
in_stream = spark.readStream.format('cloudFiles')\
            .option("cloudFiles.format", "csv")\
            .option("cloudFiles.schemaLocation", schema_location)\
            .option("cloudFiles.useIncrementalListing", "true")\
            .option("cloudFiles.backfillInterval", "1 week")\
            .load(input_path)
            

# COMMAND ----------

display(in_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### processing procedure for multiple csv 
# MAGIC 1. columns's name replacement  
# MAGIC 2. inspection_date, machine model and sales_order from ImageLink 
# MAGIC 3. ImageLink string modification, replace " " "" and replace "/"  
# MAGIC 4. time_taken from overall columns

# COMMAND ----------

type(in_stream)

# COMMAND ----------

query = in_stream.writeStream.format("delta")\
        .option("checkpointLocation", random_checkpoint_dir())\
        #.trigger(processingTime="1 minutes")\ # trigger the query for execution every 1 minute
        #.trigger(once=True) ## trigger the query for just once batch of data
        .trigger(availableNow=True) ### trigger the query for reading all available data with multiple batches
        .start(delta_path)

# COMMAND ----------

query.isActive

# COMMAND ----------

# MAGIC %md
# MAGIC #### Metadata columns
# MAGIC You can get metadata information for input files with the _metadata column. The _metadata column is a hidden column, and is available for all input file formats. To include the _metadata column in the returned DataFrame, you must explicitly reference it in your query.
# MAGIC 
# MAGIC [reference](!https://docs.databricks.com/ingestion/file-metadata-column.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### batch mode csv files test 

# COMMAND ----------

# File location and type
#file location without data consistant files 
file_location = "/dbfs/mnt/uct-landing-gen-dev/AOI/inspection_result_reformat"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

#read csv file with pandas API
import os
import re
import glob
from delta.tables import *
from datetime import datetime
import pyspark.pandas as pypd

#import necessary package 
from pyspark.sql import functions as F 
from pyspark.sql.functions import col, row_number, lit, current_timestamp
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql import Window, Row

##get all glob files 
all_csv_files = glob.glob(f"{file_location}/*/*/*.csv")
#change the batch of name of csv path 
def rename(csv):
    return csv.replace("/dbfs", "dbfs:")
all_csv_files = list(map(rename, all_csv_files))
# all_csv_files = sorted(list(map(rename, all_csv_files)), reverse=True)
print(f"all csv files:{all_csv_files}")
print(f"Number of All csv files: {len(all_csv_files)}")

# COMMAND ----------

pydf = spark.read\
        .option("inferSchema", infer_schema)\
        .option("header", first_row_is_header)\
        .option("sep", delimiter)\
        .csv(all_csv_files)\
        .select("*", "_metadata.file_modification_time", F.lit("AOI"))

# COMMAND ----------

display(pydf)

# COMMAND ----------


