# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
from pyspark import Row

# COMMAND ----------

confluentClusterName = "cluster_0"
confluentBootstrapServers = "pkc-epwny.eastus.azure.confluent.cloud:9092"
confluentTopicName = "Customerorder"
confluentApiKey = "UG6LWPUGMURPPX2N"
confluentSecret = "fZfpnG9zO53vM1Se9zPmQI8MlWm5Jd+i4i86rkLNQI1VB19fVRzIWMdkGFOC6ufl"


# COMMAND ----------

clickstreamTestDf = (
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", confluentBootstrapServers)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
  .option("kafka.ssl.endpoint.identification.algorithm", "https")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("subscribe", confluentTopicName)
#  .option("kafka.group.id", "ConsumerGroup")
  .option("startingOffsets", "latest")
  .option("failOnDataLoss", "false")
  .load())

# COMMAND ----------

schema = (StructType([
    StructField('CampaignId', StringType()),     
    StructField('CorrelationId_T', StringType()),
    StructField('CustomerAccountID', StringType()),
    StructField('CustomerOrderID', StringType()),
    StructField('OneTimeChargeAmt', StringType()),
    StructField('OrderCreatedAffiliateID', StringType()),
    StructField('OrderCreatedChannelName', StringType()),
    StructField('OrderCreatedDate', StringType()),
    StructField('OrderCreatedEmployeeID', StringType()),
    StructField('OrderCreatedRetailerID', StringType()),
    StructField('OrderDueDate', StringType()),
    StructField('OrderReasonName', StringType()),
    StructField('OrderScenario', StringType()),
    StructField('OrderStatusName', StringType()),
    StructField('OrderSubTypeName', StringType()),
    StructField('OrderSubmittedChannelName', StringType()),
    StructField('OrderSubmittedDate', StringType()),
    StructField('OrderTypeName', StringType()),
    StructField('RecurringChargeAmt', StringType()),
    StructField('SPOrganisationName', StringType()),
    StructField('Source', StringType()),
    StructField('Timestamp', StringType()),
    StructField('msgName', StringType())
         
    
    ]))

# COMMAND ----------

json_df=clickstreamTestDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

customerorder_line=json_df.withColumn("value",from_json(col("value"),schema))

customerorderline_final=(customerorder_line.select("value.CampaignId","value.CorrelationId_T","value.CustomerAccountID","value.CustomerOrderID",
                                                  "value.OneTimeChargeAmt","value.OrderCreatedAffiliateID","value.OrderCreatedChannelName",
                                                   "value.OrderCreatedDate","value.OrderCreatedEmployeeID","value.OrderCreatedRetailerID",
                                                   "value.OrderDueDate","value.OrderReasonName","value.OrderScenario","value.OrderStatusName",
                                                   "value.OrderSubTypeName","value.OrderSubmittedChannelName","value.OrderSubmittedDate",
                                                   "value.OrderTypeName", "value.RecurringChargeAmt","value.SPOrganisationName",
                                                   "value.Source","value.Timestamp","value.msgName")
                        )


display(customerorderline_final)

# COMMAND ----------

def writeTosnowflake(df,epochId):
    df.write.format("snowflake").options(**options).option("dbtable","CUSTOMER_ORDER").mode("append").save()

# COMMAND ----------

options = {
  "sfUrl": "https://kj49040.southeast-asia.azure.snowflakecomputing.com/",
  "sfUser": "DINESHTAK",
  "sfPassword": "Test@12345",
  "sfDatabase": "TESTING",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "COMPUTE_WH"
}
myStreamName="kafkaStream"

streamingQuery=(customerorderline_final
                .writeStream
                .queryName(myStreamName)
                .trigger(processingTime="3 seconds")
                .foreachBatch(writeTosnowflake)
                .start()
               )
