# Databricks notebook source
#required libs
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC Silver tranformation for customers

# COMMAND ----------

#nessa parte sera construido a tabela silver.customers incluindo algumas transformacoes e limpezas no dado.

bronze_customers_df = spark.table("bronze.customers")
silver_customers_df = (bronze_customers_df
                        .withColumn("age", F.col("age").cast("int"))
                        .withColumn("income", F.col("income").cast("int")) 
                        .fillna("N/A", subset=["gender"])
                        .withColumn("became_member_on", F.to_date(F.col("became_member_on").cast("string"), "yyyyMMdd"))
                        .withColumn("age_group",
                                     when(F.col("age") < 25, "<25")
                                    .when((F.col("age") >= 25) & (F.col("age") < 35), "25-34")
                                    .when((F.col("age") >= 35) & (F.col("age") < 50), "35-49")
                                    .when((F.col("age") >= 50) & (F.col("age") < 65), "50-64")
                                    .when((F.col("age") >= 65), "65+")
                                    .otherwise("N/A"))
                        .withColumn("income_group",
                                     when(F.col("income") < 40000, "low income")
                                    .when((F.col("income") >= 40000) & (F.col("income") <= 70000), "middle income")
                                    .when((F.col("income") > 70000), "high income")
                                    .otherwise("N/A"))
                        )

silver_customers_df.write.mode("overwrite").format("delta").saveAsTable("silver.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC Silver tranformation for offers

# COMMAND ----------

#In this part the silver.offers table will be built including some transformations and cleaning of the data.

bronze_offers_df = spark.table("bronze.offers")
array_schema = F.ArrayType(StringType())

silver_offers_df = (bronze_offers_df
                    .withColumn("difficulty", F.col("difficulty").cast("int"))
                    .withColumn("duration", F.col("duration").cast("int"))
                    .withColumn("channels", F.from_json("channels", array_schema)) #convert to array to be exploded
                    .withColumn("channel", F.explode("channels")).drop("channels") #original format ["","",""] for line
                    )

silver_offers_df.write.mode("overwrite").format("delta").saveAsTable("silver.offers")

# COMMAND ----------

# MAGIC %md
# MAGIC Silver tranformation for events

# COMMAND ----------

#In this part the silver.events table will be built including some transformations and cleaning of the data.

bronze_events_df = spark.table("bronze.events")

silver_events_df = (bronze_events_df
                    .withColumn("time", F.col("time").cast("int"))
                    .withColumn("value_json", F.regexp_replace(F.regexp_replace("value", "'", '"'), "offer id", "offer_id")) #two types of offer_id, one with spaces and one without
                    .withColumn("value_map", F.from_json("value_json", MapType(StringType(), StringType())))
                    .select("*", "value_map.offer_id", "value_map.amount", "value_map.reward") #normalizes offer_id data from row to column
                    .drop("value", "value_json", "value_map")
                    .withColumn("amount", F.col("amount").cast("decimal(10,2)")) 
                    .withColumn("reward", F.col("reward").cast("int")) 
                    )

##start of windowing data to fill offer_id to every row

#first get the max time for each customer
max_events_df = (
    silver_events_df.groupBy("customer_id")
    .agg(F.max("time").alias("max_time"))
)

#filter only 'offer received' events and add max time
offer_received_df = (
    silver_events_df.filter(F.col("event") == "offer received")
    .join(max_events_df, on="customer_id")
)

#define windown config
window_spec = Window.partitionBy("customer_id").orderBy("time")

#Applies lead(time) with fallback to max_time for last row
df_offer_received = (
    offer_received_df
    .withColumn("time_interval", F.lead("time").over(window_spec))
    .withColumn("time_interval", F.when(F.col("time_interval").isNull(), F.col("max_time")).otherwise(F.col("time_interval")))
    .select("customer_id", "offer_id", F.col("time").alias("offer_time"), "time_interval")
)

#join with original table applying time range condition to fill offer_id and add event_datetime based on time column
silver_events_df_adj = (
    silver_events_df.alias("events")
    .join(
        df_offer_received.alias("offer_received"),
        on=(
            (F.col("events.customer_id") == F.col("offer_received.customer_id")) &
            (F.col("events.time") >= F.col("offer_received.offer_time")) & (F.col("events.time") <= F.col("offer_received.time_interval"))
        ),
        how="left"
    )
    .withColumn("offer_id_alt", F.coalesce(F.col("events.offer_id"), F.col("offer_received.offer_id")))
    .withColumn("start_datetime",F.to_timestamp(F.lit("2025-06-01 00:00:00")))
    .withColumn("event_datetime", F.expr("start_datetime + make_interval(0, 0, 0, 0, time, 0, 0)"))
    .selectExpr("events.customer_id", "events.event", "events.time", "event_datetime", "events.amount", "offer_id_alt as offer_id", "events.reward")
)

#saves delta table
silver_events_df_adj.write.mode("overwrite").format("delta").saveAsTable("silver.events")