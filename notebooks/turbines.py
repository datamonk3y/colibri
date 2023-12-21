#!/usr/bin/env python
# coding: utf-8

# # Setup

# In[1]:


get_ipython().run_cell_magic('sh', '', 'python --version\n')


# In[2]:


import os
from enum import StrEnum, auto
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, FloatType
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip


# In[3]:


builder = SparkSession.builder.appName("colibri_de_test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars", "/home/jovyan/postgres/postgresql-42.7.1.jar") \
    .master("local")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# In[4]:


spark.version


# In[5]:


class Layer(StrEnum):
    RAW = auto()
    BRONZE = auto()
    SILVER = auto()
    GOLD = auto()


# In[6]:


class ColumnNames(StrEnum):
    TIMESTAMP = auto()
    TURBINE_ID = auto()
    WIND_SPEED = auto()
    WIND_DIRECTION = auto()
    POWER_OUTPUT = auto()
    DATE = auto()
    MIN_POWER_OUTPUT = auto()
    MAX_POWER_OUTPUT = auto()
    AVG_POWER_OUTPUT = auto()
    POWER_STD_DEV = auto()


# ### Get paths

# In[7]:


def get_dir(data_dir, layer):
    return f'{data_dir}/{layer}'


# In[8]:


DATA_DIR = "/home/jovyan/data"
RAW_DIR = get_dir(DATA_DIR, Layer.RAW)
BRONZE_DIR = get_dir(DATA_DIR, Layer.BRONZE)
SILVER_DIR = get_dir(DATA_DIR, Layer.SILVER)
GOLD_DIR = get_dir(DATA_DIR, Layer.GOLD)


# ### Get list of raw files

# In[9]:


raw_files = [f'{RAW_DIR}/{file}' for file in os.listdir(RAW_DIR)]
raw_files


# ### Get delta file names

# In[10]:


RAW_TURBINE = 'raw_turbine'
SUMMARY_STATS = 'summary_stats'
CLEAN_DF = 'clean_df'


# # Create Bronze Layer

# In[11]:


raw_data = spark.read.csv(raw_files, header=True)
raw_data.count()


# In[12]:


raw_data.printSchema()


# Write raw data to bronze layer in Delta format

# In[13]:


raw_data.write.format("delta").mode("overwrite").save(f'{BRONZE_DIR}/{RAW_TURBINE}')


# # Create Silver Layer

# In[14]:


bronze_data = spark.read.format('delta').load(f'{BRONZE_DIR}/{RAW_TURBINE}')


# ### Apply correct schema

# In[15]:


bronze_data.dtypes


# In[16]:


def apply_schema(df, schema):
    for column_name, type in schema.items():
        df = df.withColumn(column_name, bronze_data[column_name].cast(type))

    return df


# In[17]:


schema = {
    ColumnNames.TIMESTAMP: TimestampType(),
    ColumnNames.TURBINE_ID: IntegerType(),
    ColumnNames.WIND_SPEED: FloatType(),
    ColumnNames.WIND_DIRECTION: IntegerType(),
    ColumnNames.POWER_OUTPUT: FloatType(),
}
updated_schema_df = apply_schema(bronze_data, schema)
updated_schema_df.dtypes


# ### Check and remove rows with missing values

# In[18]:


missing_criteria = (f.col(ColumnNames.TIMESTAMP).isNull() |
                    f.col(ColumnNames.TURBINE_ID).isNull() |
                    f.col(ColumnNames.WIND_SPEED).isNull() |
                    f.col(ColumnNames.WIND_DIRECTION).isNull() |
                    f.col(ColumnNames.POWER_OUTPUT).isNull())

missing_data = updated_schema_df.where(missing_criteria)


# In[19]:


clean_df = updated_schema_df.subtract(missing_data)


# *Could also log "missing_data" dataframe into DB...*

# ### Calculate summary statistics
# 
# For each turbine, calculate the minimum, maximum, and average power output over a given time period (e.g., 24 hours).
# 
# Also calculating standard deviation based `avg_power_output` partitioned by `turbine_id` and `date`

# In[20]:


date_column = f.to_date(f.col(ColumnNames.TIMESTAMP))
std_dev_window_spec = Window.partitionBy(ColumnNames.TURBINE_ID)

summary_stats_df = (clean_df
                    .withColumn(ColumnNames.DATE, date_column)
                    .withColumn(ColumnNames.POWER_STD_DEV, f.stddev(ColumnNames.POWER_OUTPUT).over(std_dev_window_spec))
                    .groupBy(ColumnNames.DATE, ColumnNames.TURBINE_ID, ColumnNames.POWER_STD_DEV) 
                    .agg(
                        f.min(ColumnNames.POWER_OUTPUT).alias(ColumnNames.MIN_POWER_OUTPUT),
                        f.max(ColumnNames.POWER_OUTPUT).alias(ColumnNames.MAX_POWER_OUTPUT),
                        f.avg(ColumnNames.POWER_OUTPUT).alias(ColumnNames.AVG_POWER_OUTPUT),
                    )
)
summary_stats_df.show()


# # Identify Anomalies
# 
# Any turbines that have significantly deviated from their expected power output over the same time period. 
# 
# Anomalies can be defined as turbines whose output is outside of 2 standard deviations from the mean.
# 
# (All turbines seem to have been affected...)

# In[21]:


two_stddev = (f.col(ColumnNames.POWER_STD_DEV) * 2)
two_stddev_above_mean  = (f.lit(f.col('avg_power_output') + two_stddev))
two_stddev_below_mean  = (f.lit(f.col('avg_power_output') - two_stddev))

anomaly_logic = (
    (two_stddev_above_mean > f.col('max_power_output')) |
    (two_stddev_below_mean < f.col('min_power_output'))
)
    
df = (summary_stats_df.filter(anomaly_logic).select('turbine_id').distinct().orderBy('turbine_id').show())


# ### Store intermediate tables in Silver layer 

# In[22]:


summary_stats_df.write.format("delta").mode("overwrite").save(f'{SILVER_DIR}/{SUMMARY_STATS}')
clean_df.write.format("delta").mode("overwrite").save(f'{SILVER_DIR}/{CLEAN_DF}')


# # Gold Layer

# Store the cleaned data and summary statistics in a database for further analysis.

# In[23]:


def write_to_postgres(df, table_name):
    jdbc_url = "jdbc:postgresql://db:5432/postgres"
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append") \
        .save()


# In[24]:


write_to_postgres(summary_stats_df, table_name='summary')


# In[25]:


write_to_postgres(clean_df, table_name='cleaned')


# In[ ]:




