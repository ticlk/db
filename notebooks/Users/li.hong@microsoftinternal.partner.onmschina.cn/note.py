# Databricks notebook source
# MAGIC %scala
# MAGIC import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
# MAGIC 
# MAGIC // To connect to an Event Hub, EntityPath is required as part of the connection string.
# MAGIC // Here, we assume that the connection string from the Azure portal does not have the EntityPath part.
# MAGIC val connectionString = ConnectionStringBuilder("Endpoint=sb://ev1103.servicebus.chinacloudapi.cn/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=zeTe2JNde+ERUNAqkwKdr7gSSUGux773Yg6FkZleLiY=")
# MAGIC   .setEventHubName("hub02")
# MAGIC   .build
# MAGIC val eventHubsConf = EventHubsConf(connectionString)
# MAGIC   .setStartingPosition(EventPosition.fromEndOfStream)
# MAGIC 
# MAGIC var incomingStream = 
# MAGIC   spark.readStream
# MAGIC     .format("eventhubs")
# MAGIC     .options(eventHubsConf.toMap)
# MAGIC     .load()
# MAGIC 
# MAGIC incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

# COMMAND ----------

# MAGIC %scala
# MAGIC streamingInputDF.printSchema
# MAGIC streamingInputDF.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set("fs.azure.account.auth.type.storage0601.dfs.core.chinacloudapi.cn", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.storage0601.dfs.core.chinacloudapi.cn", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.storage0601.dfs.core.chinacloudapi.cn", "08715978-21cf-4596-b843-302dee41e8fd")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.storage0601.dfs.core.chinacloudapi.cn", "P~1H~Xh9AX2Pr1m9ZqJ~3Z4.01AK.ldTM3")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.storage0601.dfs.core.chinacloudapi.cn", "https://login.chinacloudapi.cn/b388b808-0ec9-4a09-a414-a7cbbd8b7e9b/oauth2/token")

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("abfss://delta@storage0601.dfs.core.chinacloudapi.cn/")

# COMMAND ----------

# MAGIC %python
# MAGIC cosmosEndpoint = "https://sql0921.documents.azure.cn:443/"
# MAGIC cosmosMasterKey = "XWk2D2ov31szQMYDNx5qQaUPPJiDbcQevbyKznnoOIJZqZYudi4qIBRfqyhbcawoElOJ1E3FKUZ1wL31SJW4jQ=="
# MAGIC cosmosDatabaseName = "AzureSampleFamilyDB"
# MAGIC cosmosContainerName = "FamilyContainer"
# MAGIC 
# MAGIC cfg = {
# MAGIC   "spark.cosmos.accountEndpoint" : cosmosEndpoint,
# MAGIC   "spark.cosmos.accountKey" : cosmosMasterKey,
# MAGIC   "spark.cosmos.database" : cosmosDatabaseName,
# MAGIC   "spark.cosmos.container" : cosmosContainerName,
# MAGIC   "spark.cosmos.read.customQuery" : "select * from c"
# MAGIC }
# MAGIC 
# MAGIC df = spark.read.format("cosmos.oltp").options(**cfg)\
# MAGIC  .option("spark.cosmos.read.inferSchema.enabled", "true")\
# MAGIC  .load()
# MAGIC  
# MAGIC display(df)

# COMMAND ----------

# MAGIC %sh
# MAGIC nslookup ev1103.servicebus.chinacloudapi.cn

# COMMAND ----------

# MAGIC %sh
# MAGIC nslookup ev1103.servicebus.chinacloudapi.cn

# COMMAND ----------

# MAGIC %python
# MAGIC from azure.eventhub import EventHubConsumerClient
# MAGIC from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
# MAGIC 
# MAGIC def on_event(partition_context, event):
# MAGIC     # Print the event data.
# MAGIC     print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))
# MAGIC 
# MAGIC     # Update the checkpoint so that the program doesn't read the events
# MAGIC     # that it has already read when you run it next time.
# MAGIC     partition_context.update_checkpoint(event)
# MAGIC 
# MAGIC checkpoint_store = BlobCheckpointStore.from_connection_string("DefaultEndpointsProtocol=https;AccountName=storage0601;AccountKey=3jRdXHldjiSmzuzQHyey3RmFGyg09wI2RoKZrnbi+FEDzN5G+DocHf63fppRqFKJrJ8q0Z5uXmoZ+ASt1H7XpQ==;EndpointSuffix=core.chinacloudapi.cn", "eventhub10")
# MAGIC 
# MAGIC client = EventHubConsumerClient.from_connection_string("Endpoint=sb://ev1103.servicebus.chinacloudapi.cn/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=zeTe2JNde+ERUNAqkwKdr7gSSUGux773Yg6FkZleLiY=", consumer_group="$Default", eventhub_name="hub02", checkpoint_store=checkpoint_store)
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC with client:
# MAGIC         # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
# MAGIC     print("recived:")
# MAGIC     client.receive(on_event=on_event,  starting_position="-1")
# MAGIC    

# COMMAND ----------

# MAGIC %sh
# MAGIC nc -zv ev1103.servicebus.chinacloudapi.cn 5671
# MAGIC nc -zv ev1103.servicebus.chinacloudapi.cn 5672
# MAGIC nc -zv ev1103.servicebus.chinacloudapi.cn 9093

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table1115

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.eventhubs._
# MAGIC import com.microsoft.azure.eventhubs._
# MAGIC 
# MAGIC val connectionString = ConnectionStringBuilder("Endpoint=sb://ev1103.servicebus.chinacloudapi.cn/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=zeTe2JNde+ERUNAqkwKdr7gSSUGux773Yg6FkZleLiY=")
# MAGIC   .setEventHubName("eventhub10")
# MAGIC   .build
# MAGIC val customEventhubParameters = EventHubsConf(connectionString)
# MAGIC   .setStartingPosition(EventPosition.fromEndOfStream)
# MAGIC 
# MAGIC 
# MAGIC val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
# MAGIC 
# MAGIC incomingStream.printSchema
# MAGIC 
# MAGIC // Sending the incoming stream into the console.
# MAGIC // Data comes in batches!
# MAGIC incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

# COMMAND ----------

# MAGIC %python
# MAGIC import org.apache.spark.eventhubs._
# MAGIC import com.microsoft.azure.eventhubs._
# MAGIC 
# MAGIC // Build connection string with the above information
# MAGIC val namespaceName = "<EVENT HUBS NAMESPACE>"
# MAGIC val eventHubName = "eventhub10"
# MAGIC val sasKeyName = "<POLICY NAME>"
# MAGIC val sasKey = "<POLICY KEY>"
# MAGIC val connStr = new com.microsoft.azure.eventhubs.ConnectionStringBuilder()
# MAGIC             .setNamespaceName(namespaceName)
# MAGIC             .setEventHubName(eventHubName)
# MAGIC             .setSasKeyName(sasKeyName)
# MAGIC             .setSasKey(sasKey)
# MAGIC 
# MAGIC val customEventhubParameters =
# MAGIC   EventHubsConf(connStr.toString())
# MAGIC   .setMaxEventsPerTrigger(5)
# MAGIC 
# MAGIC val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
# MAGIC 
# MAGIC incomingStream.printSchema
# MAGIC 
# MAGIC // Sending the incoming stream into the console.
# MAGIC // Data comes in batches!
# MAGIC incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

# COMMAND ----------

# MAGIC %sh
# MAGIC nc -zv pypi.tuna.tsinghua.edu.cn 443

# COMMAND ----------

# MAGIC %sh
# MAGIC nc -zv ev1103.servicebus.chinacloudapi.cn 5672

# COMMAND ----------

# MAGIC %sh
# MAGIC nc -zv ev1103.servicebus.chinacloudapi.cn 9093

# COMMAND ----------

# MAGIC %sh
# MAGIC nc -zv ev1103.servicebus.chinacloudapi.cn 443

# COMMAND ----------

# MAGIC %python
# MAGIC import org.apache.kafka.common.security.plain.PlainLoginModule
# MAGIC from pyspark.sql.functions import *
# MAGIC 
# MAGIC eventhub_connection_string="Endpoint=sb://wmheventhub1207.servicebus.chinacloudapi.cn/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=37ULCK3E/4EOMDkX2pBXXRg3WCwOvtIJwrXosA8SO+8="
# MAGIC 
# MAGIC consumerSaslJaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"" + eventhub_connection_string + "\";"
# MAGIC #consumerSaslJaasConfig_sink = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"" + eventhub_connection_string_sink + "\";"
# MAGIC 
# MAGIC #//earliest\latest
# MAGIC streamingInputDF = spark \
# MAGIC   .readStream \
# MAGIC   .format("kafka") \
# MAGIC   .option("kafka.bootstrap.servers", "wmheventhub1207.servicebus.chinacloudapi.cn:9093") \
# MAGIC   .option("subscribe", "eventhub001") \
# MAGIC   .option("kafka.group.id", "$Default") \
# MAGIC   .option("minPartitions", 32) \
# MAGIC   .option("maxOffsetsPerTrigger", 100) \
# MAGIC   .option("kafka.sasl.mechanism",  "PLAIN") \
# MAGIC   .option("kafka.security.protocol", "SASL_SSL") \
# MAGIC   .option("kafka.sasl.jaas.config", consumerSaslJaasConfig) \
# MAGIC   .option("kafka.request.timeout.ms",  "60000") \
# MAGIC   .option("kafka.session.timeout.ms","60000") \
# MAGIC   .option("failOnDataLoss",  "false") \
# MAGIC   .option("startingOffsets","earliest") \
# MAGIC   .load()
# MAGIC 
# MAGIC streamingSelectDF = (
# MAGIC   streamingInputDF
# MAGIC    .selectExpr("value::string:timestamp")
# MAGIC    .groupBy("timestamp") 
# MAGIC    .count()
# MAGIC   )
# MAGIC 
# MAGIC display(streamingSelectDF)

# COMMAND ----------

streamingSelectDF = (
  streamingInputDF
   .selectExpr("value::string:name")
   .groupBy("name") 
   .count()
  )

display(streamingSelectDF)

# COMMAND ----------

disply

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE table1212
# MAGIC   (id int,  fname string)
# MAGIC   USING DELTA
# MAGIC   LOCATION 'abfss://blobtest@storagegen21212.dfs.core.chinacloudapi.cn/table1212'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table1212(id, fname) values(6,'test6');
# MAGIC insert into table1212(id, fname) values(2,'test2');
# MAGIC insert into table1212(id, fname) values(3,'test3');
# MAGIC insert into table1212(id, fname) values(4,'test4');
# MAGIC insert into table1212(id, fname) values(5,'test5');

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.unmount("/mnt/mount01")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table1115;
# MAGIC insert into table1115(id, fname) values(2,'test2')

# COMMAND ----------

# MAGIC %sh 
# MAGIC nc -zv login.partner.microsoftonline.cn 443

# COMMAND ----------

# MAGIC %sh 
# MAGIC nc -zv storagegen21212.dfs.core.chinacloudapi.cn 443

# COMMAND ----------

# MAGIC %sh
# MAGIC curl -X POST https://login.partner.microsoftonline.cn/b388b808-0ec9-4a09-a414-a7cbbd8b7e9b/oauth2/token \
# MAGIC -H 'cache-control: no-cache' \
# MAGIC -H 'content-type: multipart/form-data' \
# MAGIC -F 'client_id=08715978-21cf-4596-b843-302dee41e8fd ' \
# MAGIC -F 'client_secret=-I2juv..8m5XGlEV-3ebOs~W6l42j44irQ' \
# MAGIC -F 'grant_type=client_credentials' \
# MAGIC -F 'resource=https://storage.azure.com/'

# COMMAND ----------

# MAGIC %sh
# MAGIC access_token="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Il9DSkFPdHlzWVZtNXhjMVlvSzBvUTdxeUJDUSIsImtpZCI6Il9DSkFPdHlzWVZtNXhjMVlvSzBvUTdxeUJDUSJ9.eyJhdWQiOiJodHRwczovL3N0b3JhZ2UuYXp1cmUuY29tLyIsImlzcyI6Imh0dHBzOi8vc3RzLmNoaW5hY2xvdWRhcGkuY24vYjM4OGI4MDgtMGVjOS00YTA5LWE0MTQtYTdjYmJkOGI3ZTliLyIsImlhdCI6MTY3MTE4OTU4MiwibmJmIjoxNjcxMTg5NTgyLCJleHAiOjE2NzEyMTg2ODIsImFpbyI6IjQySmdZS2p3ZnJWdnc1YTBiN3dMMlpkVW52SCtCZ0E9IiwiYXBwaWQiOiIwODcxNTk3OC0yMWNmLTQ1OTYtYjg0My0zMDJkZWU0MWU4ZmQiLCJhcHBpZGFjciI6IjEiLCJpZHAiOiJodHRwczovL3N0cy5jaGluYWNsb3VkYXBpLmNuL2IzODhiODA4LTBlYzktNGEwOS1hNDE0LWE3Y2JiZDhiN2U5Yi8iLCJvaWQiOiIwNzc5NWM5MS1lMmJkLTQ5YTMtYjRlYy1jMzA1YjNmNjYxNGUiLCJyaCI6IjAuQVFJQUNMaUlzOGtPQ1Vxa0ZLZkx2WXQtbTRHbUJ1VFU4NmhDa0xiQ3NDbEpldkVCQUFBLiIsInN1YiI6IjA3Nzk1YzkxLWUyYmQtNDlhMy1iNGVjLWMzMDViM2Y2NjE0ZSIsInRpZCI6ImIzODhiODA4LTBlYzktNGEwOS1hNDE0LWE3Y2JiZDhiN2U5YiIsInV0aSI6Imw2a3d2RTRJS0VTM3NmSzZXY0MyQUEiLCJ2ZXIiOiIxLjAifQ.rn4Iu2hJGOYM6ElTDmdm1ietP6FiSSuhZvy3J_P2LfLrxEuwyJ-2grMvPIsE98fggRBYf-zBGe1LVYRlk-oepPHAvh4aXFHtfjJNzuiq3VnRkHEO6oSJ3PEUQhK7z2yRqNCOqhYH_rj8QirsraAEjgHXrdpxEWyjjI-ASfo34E7FFX0YLloMXvMRSzSKLNaAC2JFVoQsP2Kh8NeQ6gb9zeighNhgKmwwXynwP4ek0EJIb_G_UB3ihhVpLzmJBn5vYQnZ9Yvreuvru-0_v8rtGfFrAUM0_bD-3yE4sfMDv3go8ig8eyZAH2pm_RbYPI4lSSJg1Pa50DFSxD_ovDnMEQ"
# MAGIC 
# MAGIC echo $access_token

# COMMAND ----------

# MAGIC %sh
# MAGIC curl -H "x-ms-version: 2018-11-09" -H "Authorization: Bearer $access_token" https://storagegen21212.dfs.core.chinacloudapi.cn/blobtest?recursive=false&resource=filesystem

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks/common/conf/")