# Databricks notebook source
#required libs
import pyspark.sql.functions as F
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC Read data source

# COMMAND ----------

#read silver table 
customers = spark.read.table("silver.customers")
offers =    spark.read.table("silver.offers")
events =    spark.read.table("silver.events")

# COMMAND ----------

# MAGIC %md
# MAGIC create dim_customer 

# COMMAND ----------

#creates and write the dimension using related data
dim_customer = (customers
                .select("customer_id", "age", "gender", "income", "became_member_on", "age_group", "income_group")
                .dropDuplicates(["customer_id"])
                )

#due to the low volume of data I decided to use "overwrite"
(dim_customer.write
    .mode("overwrite")
    .partitionBy("gender", "became_member_on")
    .format("delta")
    .saveAsTable("gold.dim_customer")
)

# COMMAND ----------

# MAGIC %md
# MAGIC create dim_offer

# COMMAND ----------

#creates and write the dimension using related data
dim_offer = offers.select("offer_id", "offer_type", "difficulty", "duration").dropDuplicates()

#due to the low volume of data I decided to use "overwrite"
(dim_offer.write.mode("overwrite")
    .partitionBy("offer_type")
    .format("delta")
    .saveAsTable("gold.dim_offer")
)

# COMMAND ----------

# MAGIC %md
# MAGIC create dim_channel

# COMMAND ----------

#creates and write the dimension and adds an id (md5) to each record
dim_channel = (offers.select("channel")
               .dropDuplicates()
               .withColumn("channel_id", F.md5(F.col("channel")))
               .select("channel_id", "channel")
            )
            
#due to the low volume of data I decided to use "overwrite"
(dim_channel.write.mode("overwrite")
    .format("delta")
    .saveAsTable("gold.dim_channel")
)

# COMMAND ----------

# MAGIC %md
# MAGIC create dim_event

# COMMAND ----------

#creates and write the dimension and adds an id to each record
dim_event = (events.select("event")
               .dropDuplicates()
               .withColumn("event_id", F.md5(F.col("event")))
               .select("event_id", "event")
            )

#due to the low volume of data I decided to use "overwrite"
(dim_event.write.mode("overwrite")
    .format("delta")
    .saveAsTable("gold.dim_event")
)

# COMMAND ----------

# MAGIC %md
# MAGIC create fact

# COMMAND ----------

#create and write fact_table with join
fact_offer_events = (events.alias("e")
                    .join(offers.alias("o"), "offer_id", "left")
                    .join(customers.alias("c"), "customer_id", "left")
                    .withColumn("channel_id", F.md5(F.col("channel")))
                    .withColumn("event_id", F.md5(F.col("event")))
                    .selectExpr(
                        "e.customer_id",
                        "e.offer_id",
                        "channel_id",
                        "event_id",
                        "e.reward",
                        "e.time",
                        "e.event_datetime",
                        "e.amount"
                    ))

#due to the low volume of data I decided to use "overwrite"
(fact_offer_events.write.mode("overwrite")
    .partitionBy("event_id", "channel_id")
    .format("delta")
    .saveAsTable("gold.fact_events")
)