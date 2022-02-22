# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
from pyspark import Row
import datetime
import os 

# COMMAND ----------

confluentClusterName = "cluster_0"
confluentBootstrapServers = "pkc-epwny.eastus.azure.confluent.cloud:9092"
confluentTopicName = "createaccount"
confluentApiKey = "UG6LWPUGMURPPX2N"
confluentSecret = "fZfpnG9zO53vM1Se9zPmQI8MlWm5Jd+i4i86rkLNQI1VB19fVRzIWMdkGFOC6ufl"


# COMMAND ----------

createaccountTestDf = (
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
    StructField('AccountInfo', StructType([
        StructField('AccountCreatedDate', StringType()),
        StructField('AccountEndDate', StringType()),
        StructField('AccountID', StringType()),
        StructField('AccountStatusName', StringType()),
        StructField('AccountTypeName', StringType()),
        StructField('BillingAccount', StructType([
            StructField('AssociatedAccountID', StringType()),
            StructField('BillCycleDay', StringType()),
            StructField('BillDeliveryMethod',  ArrayType(StructType([
                StructField('BillDeliveryMethodName', StringType())]))),
            StructField('BillReportTypeName', StringType()),
            StructField('ItemisedBillFlag', StringType()),
            StructField('VATBillFlag', StringType())])),
        StructField('LoanAccount', StructType([
            StructField('AssociatedAccountID', StringType()),
            StructField('LoanAgreementID', StringType())])),
        StructField('PartyRelationshipInfo',ArrayType(StructType([
            StructField('AccountPartyStatusEndDate', StringType()),
            StructField('PartyID', StringType()),
            StructField('PartyRoleTypeName', StringType())]))),
        StructField('PaymentPlan', StructType([
            StructField('DirectDebitSignDate', StringType()),
            StructField('PaymentMethodName',ArrayType(StructType([
                StructField('PaymentMethodName', StringType()),
                StructField('PaymentMethodStatus', StringType()),
                StructField('PaymentPlanEffectiveDate', StringType())]))),
            StructField('SPOrganisationName', StringType())])),
         StructField('CorrelationId_T', StringType())])),
    StructField('PartyInfo',ArrayType(StructType([
        StructField('FraudFlag', StringType()),
        StructField('Individual', StructType([
            StructField('DeceasedFlag', StringType()),
            StructField('FamilyName', StringType()),
            StructField('Gender', StringType()),
            StructField('GivenName', StringType()),
            StructField('MaritalStatusName', StringType()),
            StructField('PartySpecialNeed',ArrayType(StructType([
                StructField('PartySpecialNeedName', StringType())]))),
            StructField('Salutation', StringType())])),
            StructField('PartyContactMedium',ArrayType(StructType([
                StructField('Address',ArrayType(StructType([
                     StructField('ContactMediumTypeName', StringType()),
                     StructField('ContactMediumTypeValue', StructType([
                         StructField('AddressInfo', StructType([
                             StructField('PostalAddressFormat', StructType([
                                  StructField('PAFAddressLine1', StringType()),
                                  StructField('PAFAddressLine10', StringType()),
                                  StructField('PAFAddressLine11', StringType()),
                                  StructField('PAFAddressLine2', StringType()),
                                  StructField('PAFAddressLine3', StringType()),
                                  StructField('PAFAddressLine5', StringType()),
                                  StructField('PAFAddressLine8', StringType()),
                                  StructField('PAFAddressLine9', StringType())
                             ])),
                             StructField('RebusAddress', StructType([
                                  StructField('RebusAddressLine1', StringType()),
                                  StructField('RebusAddressLine2', StringType()),
                                  StructField('RebusCountryName', StringType()),
                                  StructField('RebusCounty', StringType()),
                                  StructField('RebusPostCode', StringType()),
                                  StructField('RebusTown', StringType()), 
                             ])),
                         ])),
                     ])),                    
                ]))),
                StructField('ContactMediumType',ArrayType(StructType([
                    StructField('ContactMediumTypeName', StringType()),
                    StructField('ContactMediumTypeValue', StringType())
                ]))),
                 StructField('PartyRoleTypeName', StringType()),
            ]))),
            StructField('PartyEndDate', StringType()),
            StructField('PartyID', StringType()),
            StructField('SPOrganisationName', StringType()),
            StructField('VIPFlag', StringType())
        ]))),
    StructField('Source', StringType()),
    StructField('Timestamp', StringType()),
    StructField('msgName', StringType())
]))


# COMMAND ----------

json_df=createaccountTestDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

CreateAccount=json_df.withColumn("value",from_json(col("value"),schema))

AccountInfo=(CreateAccount
             .withColumn("BillDeliveryMethod1",explode("value.AccountInfo.BillingAccount.BillDeliveryMethod"))
             .withColumn("PartyRelationshipInfo1",explode("value.AccountInfo.PartyRelationshipInfo"))
             .withColumn("PaymentMethodName1",explode("value.AccountInfo.PaymentPlan.PaymentMethodName"))
             .withColumn("PartyInfo",explode("value.PartyInfo"))
             .withColumn("PartySpecialNeed1",explode("PartyInfo.Individual.PartySpecialNeed"))
             .withColumn("PartyContactMedium",explode("PartyInfo.PartyContactMedium"))
             .withColumn("Address",explode(col("PartyContactMedium.Address")))
             .withColumn("ContactMediumType",explode(col("PartyContactMedium.ContactMediumType")))
             .select(
                 col("value.AccountInfo.AccountCreatedDate"),
                 col("value.AccountInfo.AccountEndDate"),
                 col("value.AccountInfo.AccountID"),
                 col("value.AccountInfo.AccountStatusName"),
                 col("value.AccountInfo.AccountTypeName"),
                 col("value.AccountInfo.BillingAccount.AssociatedAccountID"),
                 col("value.AccountInfo.BillingAccount.BillCycleDay"),
                 col("BillDeliveryMethod1.*"),
                 col("value.AccountInfo.BillingAccount.BillReportTypeName"),
                 col("value.AccountInfo.BillingAccount.ItemisedBillFlag"),
                 col("value.AccountInfo.BillingAccount.VATBillFlag"),
                 col("value.AccountInfo.LoanAccount.AssociatedAccountID").alias("LoanAccount_AssociatedAccountID"),
                 col("value.AccountInfo.LoanAccount.LoanAgreementID"),
                 col("PartyRelationshipInfo1.*"),
                 col("value.AccountInfo.PaymentPlan.DirectDebitSignDate"),
                 col("value.AccountInfo.PaymentPlan.SPOrganisationName:").alias("PaymentPlan_SPOrganisationName"),
                 col("PaymentMethodName1.PaymentMethodName").alias("PaymentMethodName"),
                 col("PaymentMethodName1.PaymentMethodStatus").alias("PaymentMethodStatus"),
                 col("PaymentMethodName1.PaymentPlanEffectiveDate").alias("PaymentPlanEffectiveDate"),
                 col("PartyInfo.FraudFlag").alias('FraudFlag'),
                 col("PartyInfo.Individual.DeceasedFlag").alias('DeceasedFlag'),
                 col("PartyInfo.Individual.FamilyName").alias('FamilyName'),
                 col("PartyInfo.Individual.Gender").alias('Gender'),
                 col("PartyInfo.Individual.GivenName").alias('GivenName'),
                 col("PartyInfo.Individual.MaritalStatusName").alias('MaritalStatusName'),
                 col("PartySpecialNeed1.PartySpecialNeedName"),
                 col("Address.ContactMediumTypeName").alias("ContactMediumTypeName"),
                 col("Address.ContactMediumTypeValue.AddressInfo.PostalAddressFormat.PAFAddressLine1").alias("PAFAddressLine1"),
                 col("Address.ContactMediumTypeValue.AddressInfo.PostalAddressFormat.PAFAddressLine10").alias("PAFAddressLine10"),
                 col("Address.ContactMediumTypeValue.AddressInfo.PostalAddressFormat.PAFAddressLine11").alias("PAFAddressLine11"),
                 col("Address.ContactMediumTypeValue.AddressInfo.PostalAddressFormat.PAFAddressLine2").alias("PAFAddressLine2"),
                 col("Address.ContactMediumTypeValue.AddressInfo.PostalAddressFormat.PAFAddressLine3").alias("PAFAddressLine3"),
                 col("Address.ContactMediumTypeValue.AddressInfo.PostalAddressFormat.PAFAddressLine5").alias("PAFAddressLine5"),
                 col("Address.ContactMediumTypeValue.AddressInfo.PostalAddressFormat.PAFAddressLine8").alias("PAFAddressLine8"),
                 col("Address.ContactMediumTypeValue.AddressInfo.PostalAddressFormat.PAFAddressLine9").alias("PAFAddressLine9"),
                 col("Address.ContactMediumTypeValue.AddressInfo.RebusAddress.RebusAddressLine1").alias("RebusAddressLine1"),
                 col("Address.ContactMediumTypeValue.AddressInfo.RebusAddress.RebusAddressLine2").alias("RebusAddressLine2"),
                 col("Address.ContactMediumTypeValue.AddressInfo.RebusAddress.RebusCountryName").alias("RebusCountryName"),
                 col("Address.ContactMediumTypeValue.AddressInfo.RebusAddress.RebusCounty").alias("RebusCounty"),
                 col("Address.ContactMediumTypeValue.AddressInfo.RebusAddress.RebusPostCode").alias("RebusPostCode"),
                 col("Address.ContactMediumTypeValue.AddressInfo.RebusAddress.RebusTown").alias("RebusTown"),
                 col("ContactMediumType.ContactMediumTypeName").alias("ContactMediumType_ContactMediumTypeName"),
                 col("ContactMediumType.ContactMediumTypeValue").alias("ContactMediumType_ContactMediumTypeValue"),
                 col("PartyInfo.PartyEndDate").alias('PartyEndDate'),
                 col("PartyInfo.PartyID").alias('partyinfo_PartyID'),
                 col("PartyInfo.SPOrganisationName").alias('PartyInfo_SPOrganisationName'),
                 col("PartyInfo.VIPFlag").alias('VIPFlag'),
                 col("value.Source"),
                 col("value.Timestamp"),
                 col("value.msgName")         
                )
            )
       
display(AccountInfo)

# COMMAND ----------

def writeTosnowflake(df,epochId):
    df.write.format("snowflake").options(**options).option("dbtable","Create_Account").mode("append").save()

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

streamingQuery=(AccountInfo
                .writeStream
                .queryName(myStreamName)
                .trigger(processingTime="3 seconds")
                .foreachBatch(writeTosnowflake)
                .start()
               )

