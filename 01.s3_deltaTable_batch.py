# Databricks notebook source
# MAGIC %md
# MAGIC #### ETL pipeline
# MAGIC example for reading mannully uploaded test data and transform it to the desired format 
# MAGIC - input csv file 
# MAGIC - transformation: delete useless columns and add additional columns (model,sales_order,inspection_date,time_taken)
# MAGIC - output delta table 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### I/O config

# COMMAND ----------

# MAGIC %md
# MAGIC ###### input config

# COMMAND ----------

# File location and type
#file location without data consistant files 
file_location = "/dbfs/mnt/uct-landing-gen-dev/AOI/inspection_result_reformat"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# COMMAND ----------

# MAGIC %md
# MAGIC ###### output config

# COMMAND ----------

#output config for writing data into delta table
#/dbfs/mnt/uct-landing-gen-dev/AOI/inspection_result/ACDC/575-341084-E8242_28082022_1310/575-341084-E8242_28082022_1310.csv
uct_transform_gen = spark.sql("select variable_value from Config.config_constant where variable_name = 'uct-transform-gen'").first()[0]

table_name = "aoi_inspection_result_batch"
write_format = "delta"
location = "AOI"
data_source = "AOI" #specify the name of data source, eg. AOI, AOI_1? 
write_path = f"/mnt/{uct_transform_gen}/{location}/{table_name}"
# dbutils.fs.rm(write_path, recurse=True)

# COMMAND ----------

# dbfs_file_location = file_location.replace("/dbfs", "dbfs:")
# # print(dbutils.fs.ls(dbfs_file_location))

# print(dbutils.fs.rm(dbfs_file_location, recurse=True))
# print(dbutils.fs.rm(write_path, recurse=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Reading

# COMMAND ----------

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

#change the batch of name of csv path 
def rename(csv):
    return csv.replace("/dbfs", "dbfs:")

##get all glob files 
all_csv_files = glob.glob(f"{file_location}/*/*/*.csv")
all_csv_files = list(map(rename, all_csv_files))

# print(f"Number of All csv files: {len(all_csv_files)}")
# print(f"all csv files:{all_csv_files}")


# COMMAND ----------

# MAGIC %md
# MAGIC csv file problem 
# MAGIC 1. header of csv files are not consistant, 575-313788-CC5559_15032021_1122(Result) with the recent ones(InspectedResult); 
# MAGIC 2. Mising Data

# COMMAND ----------

# pydf = pypd.read_csv(file_location, sep=",")
# read csv with pandas 
#read with dataframe 
pydf = spark.read\
        .option("inferSchema", infer_schema)\
        .option("header", first_row_is_header)\
        .option("sep", delimiter)\
        .csv(all_csv_files)\
        .select("*",  col("_metadata.file_modification_time").alias("ingestion_date"), F.lit(data_source).alias("data_source"))

# COMMAND ----------

display(pydf.where(col("ImageLink").isNull()))

# COMMAND ----------

print(pydf.count())
display(pydf)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### preprocessing
# MAGIC - Get the summary info from the csv file (overall time taken, passed, failed)
# MAGIC - delete the redundant columns and add additional columns(model, inspection_date, sales_order, time_taken)

# COMMAND ----------

#string split function with udf 
dict_schema = StructType()\
    .add("image_link", StringType(), nullable=True)\
    .add("model", StringType(), nullable=True)\
    .add("sale_order", StringType(), nullable=True)\
    .add("inspection_date", StringType(), nullable=True)

@F.udf(returnType=dict_schema)
def extract_info(imglink):
    print(imglink)
    #case1: imglink=HYPERLINK("Q:\Inspection Results\MPD2\575-313788-CE5900A_13112022_1633\SO VERIFICATION.bmp")
    #case2: imglink=Q:\\Inspection Results\\ACDC\\575-341084-F0014_30082022_1728\\DoorPanelSO.bmp
    #need to validate of path 
    try:
        items = imglink.split('"')[1].split("\\")[-3:]
    except: 
        try:
            items = imglink.split("\\")[-3:]
        except:
            #Handle None type case
            return {"image_link": None,\
                    "model":None, \
                    "sale_model":None,\
                    "inspection_date":None}
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

pydf_addInfo = pydf.withColumn("newCol", extract_info(pydf["ImageLink"]))\
    .select(\
         col("Recipe Location").alias("recipe_location"),\
         col("Recipe").alias("recipe"),\
         col("Name").alias("name"),\
         col("Program").alias("program"),\
         col("InspectedResult").alias("inspection_result"),\
         col("ExpctedResult").alias("expected_result"),\
         col("ComponentName").alias("component_name"),\
         col("newCol").image_link.alias("image_link"),\
         col("newCol").sale_order.alias("sale_order"),\
         col("newCol").model.alias("model"),\
         F.to_timestamp(\
                        col("newCol").inspection_date).alias("inspection_date"),\
         col("Pass/Fail").alias("pass_fail"),\
         col("Overwrite").alias("overwrite"),\
         col("Overall").alias("overall"),\
           "ingestion_date", "data_source")

# COMMAND ----------

print(pydf_addInfo.count())
display(pydf_addInfo)

# COMMAND ----------

##method1: doesn't work 
### get certain rows with partitionBy and filter method 
# w1 = Window.partitionBy("inspection_date").orderBy(col("overall").desc())
# display(pydf_addInfo.withColumn("row", row_number().over(w1))\
#         .filter(col("row").isin([1,2,3])))

## method2: toPandas 
## match time r'\d{2}:\d{2}:\d{2}'
## df['level'].fillna(df['level'].where(df['level'].str.contains('MG|EE')).groupby(df['email']).bfill())
# pd_df = pydf_addInfo.toPandas()
# pd_df["overall"] = pd_df["overall"].replace(\
#         pd_df["overall"].where(pd_df['overall'].str.match(r"\d{2}:\d{2}:\d{2}"))\
#         .groupby(pd_df["inspection_date"]).ffill())

# two_cols = pydf_addInfo.select("overall", "inspection_date").where(pydf_addInfo["overall"].rlike('[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}'))
# pydf_addInfo_1 = pydf_addInfo.withColumn("overall2", lit("test"))\
#     .where(two_cols["inspection_date"] == pydf_addInfo["inspection_date"])
# pydf_addInfo_1 = pydf_addInfo.withColumn("overall2",\
#                      F.when(two_cols["inspection_date"] == pydf_addInfo["inspection_date"], \
#                          two_cols["overall"]))

## filter out the overall time taken from overall column for 
## each group(different inspection time) and join it with original dataframe
## 
two_cols = pydf_addInfo.select(\
               F.unix_timestamp("overall","HH:mm:ss.SS").alias("time_taken"),\
               col("inspection_date").alias("i_date"))\
                .where(pydf_addInfo["overall"].rlike('[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}'))

pydf_addInfo_1 = pydf_addInfo.join(two_cols,\
                                two_cols.i_date == pydf_addInfo.inspection_date,\
                                "left").drop("overall", "i_date")

# COMMAND ----------

display(two_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing DataFrame to Delta Table

# COMMAND ----------

print(pydf_addInfo_1.count())
display(pydf_addInfo_1)
# display(two_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### create structType
# MAGIC create structType for pyspark DataFrame

# COMMAND ----------

# MAGIC %md 
# MAGIC since this is full load and write approach, we use overwrite mode

# COMMAND ----------

# dbutils.fs.rm(write_path,recurse=True)
# DeltaTable.isDeltaTable(spark, write_path)
pydf_addInfo_1.write.format(write_format).mode("overwrite").save(write_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Delta table test

# COMMAND ----------

test_df = spark.read.load(write_path)
display(test_df.groupBy("sale_order","inspection_date").count())

# COMMAND ----------

# display(test_df.where(col("time_taken").isNull()))

# COMMAND ----------

# display(test_df.groupBy("inspection_date").count())
