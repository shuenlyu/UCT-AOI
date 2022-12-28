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
file_location = "/FileStore/tables/AOI_test_dataset/ACDC/575-341084-E8242_28082022_1310/575_341084_E8242_28082022_1310.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# COMMAND ----------

# MAGIC %md
# MAGIC ###### output config

# COMMAND ----------

#output config for writing data into delta table
#/dbfs/mnt/uct-landing-gen-dev/AOI/inspection_result/ACDC/575-341084-E8242_28082022_1310/575-341084-E8242_28082022_1310.csv
uct_transform_gen = spark.sql("select variable_value from Config.config_constant where variable_name = 'uct-transform-gen'").first()[0]

table_name = "aoi_inspection_result"
write_format = "delta"
location = "AOI"
write_path = f"/mnt/{uct_transform_gen}/{location}/{table_name}"

stage_view = f"stg_{table_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Reading

# COMMAND ----------

#read csv file with pandas API
import pyspark.pandas as pypd
import re
from datetime import datetime 
from delta.tables import *
pydf = pypd.read_csv(file_location, sep=",")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### preprocessing
# MAGIC - Get the summary info from the csv file (overall time taken, passed, failed)
# MAGIC - delete the redundant columns and add additional columns(model, inspection_date, sales_order, time_taken)

# COMMAND ----------

def num_filter(x):
    """
    extract the number from '95item pass', '14 item failed'
    """
    return int(re.findall("\d+", x)[0])

def get_overall_data(df):
    ##get the overall time
    ##get total passed No. items and failed No. items 
    ##get final passed No. items and failed No. after overwrite
    #prefix bo: before overwirte, ao: after overwrite
    
    overall_bo = df["Overall"]
    tt_bo = [float(i) for i in overall_bo[0].split(":")]
    #calculate the total time taken in seconds
    time_taken_bo = 3600 * tt_bo[0] + 60 * tt_bo[1] + tt_bo[2]
    passed_no_bo = num_filter(overall_bo[1])
    failed_no_bo = num_filter(overall_bo[2])
    print(f"Before overwrite, Overall time taken: {time_taken_bo}s, \
             Passed Items: {passed_no_bo},\
             Failed Items: {failed_no_bo}")
    
    overall_ao = df["After Overwrite"]
    passed_no_ao = num_filter(overall_ao[0])
    failed_no_ao = num_filter(overall_ao[1])
    print(f"After Overwrite, Overall Passed items: {passed_no_ao},\
            Failed Items: {failed_no_ao}")
    return time_taken_bo, passed_no_bo, failed_no_bo,\
        passed_no_ao, failed_no_ao
    
time_taken_bo, passed_no_bo, failed_no_bo,\
    passed_no_ao, failed_no_ao = get_overall_data(pydf)
    

# COMMAND ----------

## get the model and sales no from the name of csv file 
def datetime_format(date, time):
    dt = datetime.strptime(f"{date}_{time}", "%d%m%Y_%H%M")
    dt_str = datetime.strftime(dt, "%Y-%m-%d %H:%M")
    return dt_str

def get_model_SN_ID(file_path):
    #get the model and sales no and inspection date from the file path 
    model, items = file_path.split("/")[-3:-1]
    print(items, model)
    items = items.split('_')
    sales_no = items[0]
    
    #date time
    #using re findall to filter all those have character in the date 
    date = re.findall("\d+", items[1])[0]
    time = items[2]
    date_time = datetime_format(date, time)
    print(f"Sales No: {sales_no}, model: {model}, inspection_date: {date_time}")
    return model, sales_no, date_time 
model, sales_no, inspection_date = get_model_SN_ID(file_location)

# COMMAND ----------

#delete columns and add new columns with info 
pydf.head(5)

# COMMAND ----------

##drop Overall and 'After Overwrite' columns 
## Overwrite columns fill null with ""
##ImageLink cut short the string to 
##columns name replacement 
##add new columns 
def rewrite_image_link(txt):
    items = txt.split('"')[1].split("\\")[-3:]
    return "\\".join(items)
    
pydf_after = pydf.drop(["Overall", "After Overwrite"], axis=1)
pydf_after.fillna({"Overwrite":""}, inplace=True)
pydf_after["ImageLink"] = pydf_after["ImageLink"].map(rewrite_image_link)
new_columns_name = ["recipe_location", "recipe", "name", "program", "inspected_result",\
                   "expected_result", "component_name", "image_link", 'pass_fail',\
                   'overwrite']
pydf_after.columns = new_columns_name
pydf_after["model"] = model
pydf_after['sales_order'] = sales_no 
pydf_after['time_taken'] = time_taken_bo 
pydf_after["inspected_date"] = inspection_date


# COMMAND ----------

pydf_after.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### pandas.DataFrame to pyspark.DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ###### create structType
# MAGIC create structType for pyspark DataFrame

# COMMAND ----------

pydf_after.columns.values
pydf_after["inspected_date"] = pydf_after["inspected_date"].astype('datetime64[ns]')

# COMMAND ----------

pydf_after.head(10)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *

schema = StructType().\
        add("recipe_location", StringType(), True).\
        add("recipe", StringType(), True).\
        add("name", StringType(), True).\
        add("program", StringType(), True).\
        add("inspected_result", StringType(), True).\
        add("expected_result", StringType(), True).\
        add("component_name", StringType(), True).\
        add("image_link", StringType(), True).\
        add("pass_fail", StringType(), True).\
        add("overwrite", StringType(), True).\
        add("model", StringType(), True).\
        add("sales_order", StringType(), True).\
        add("time_token", FloatType(), True).\
        add("inspected_date", TimestampType(), True)

# COMMAND ----------

spark = SparkSession.builder.appName("AOI-inspection-result").getOrCreate()
df = spark.createDataFrame(pydf_after.to_pandas(), schema=schema)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark, write_path) == False):
    df.write.format(write_format).mode("overwrite").save(write_path)

# COMMAND ----------

dbutils.fs.rm("/mnt/uct-transform-gen-dev/AOI/aoi_inspection_result",recurse=True)

# COMMAND ----------

df.createTempView(stage_view)

# COMMAND ----------

stage_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stg_aoi_inspection_result
