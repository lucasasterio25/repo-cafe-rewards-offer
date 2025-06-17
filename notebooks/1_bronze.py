# Databricks notebook source
# MAGIC %pip install kaggle

# COMMAND ----------

#required libs
import os
import zipfile
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC Download files from Kaggle - Café Rewards Offer

# COMMAND ----------

#volume already created and with kaggle.json file extracted from kaggle API
volume_path = "/Volumes/workspace/bronze/cafe_rewards"
os.environ['KAGGLE_CONFIG_DIR'] = volume_path

#set path to recieve downloaded data
download_path = os.path.join(volume_path, "download")
os.makedirs(download_path, exist_ok=True)

#download data from API
!kaggle datasets download -d arshmankhalid/caf-rewards-offer-dataset -p {download_path} --force

#extract zip file
with zipfile.ZipFile(os.path.join(download_path, "caf-rewards-offer-dataset.zip"), 'r') as zip_ref:
    zip_ref.extractall(download_path)


# COMMAND ----------

# MAGIC %md
# MAGIC Read data from volume and save to bronze layer

# COMMAND ----------

#function created to reuse merge logic
def merge_new_data(source_df, target_table_name, join_columns):
    target_table = DeltaTable.forName(spark, target_table_name)
    join_condition = " AND ".join([f"source.{col} = target.{col}" for col in join_columns])

    (target_table.alias("target")
     .merge(source_df.alias("source"), join_condition)
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
     )

#read downloaded files into dataframe
customers = spark.read.option("header", True).csv(download_path + "/customers.csv")
offers = spark.read.option("header", True).csv(download_path + "/offers.csv")
events = spark.read.option("header", True).csv(download_path + "/events.csv").distinct()

#merge old data with new data
merge_new_data(customers, "bronze.customers", ["customer_id"])
merge_new_data(offers, "bronze.offers", ["offer_id"])
merge_new_data(events, "bronze.events", ["customer_id","event","time","value"])