# Databricks notebook source
# MAGIC %run ../utils/mount_storage

# COMMAND ----------

from pyspark.sql.functions import lit, col
from datetime import datetime

load_time = datetime.now()
raw_base_path = dbutils.secrets.get("demo", "raw-datalake-path") + "cu"
refined_base_path = dbutils.secrets.get("demo", "refined-datalake-path") + "cu"
raw_format = "parquet"
refined_format = "delta"


adls_authenticate()

# COMMAND ----------

# def adls_authenticate():
#   """
#   Dependencies:`
#     - Service principal is created and assigned permission
#     - Secret scope created using Azure Key Vault or Databricks Secret Scope
#     - Key values are added to the secret scope so that references from dbutils.secrets.get work properly
#   """
#   secret_scope_name = "demo"
#   account_name = "dlpssa4dev04"
#   directory_id = "9fb75466-59fe-466b-bebb-b5805c51aaba"
#   client_id = dbutils.secrets.get(secret_scope_name, 'service-principle-id') 
# #   app_id = dbutils.secrets.get(secret_scope_name, '')
 
#   credential = dbutils.secrets.get(secret_scope_name, 'service-principle-pwd')
  
#   spark.conf.set("fs.azure.account.auth.type", "OAuth")
#   spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
#   spark.conf.set("fs.azure.account.oauth2.client.id", client_id)
#   spark.conf.set("fs.azure.account.oauth2.client.secret", credential)
#   spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/{0}/oauth2/token".format(directory_id))

# COMMAND ----------


#Area table
area_raw_df = spark.read.format(raw_format).load(raw_base_path + "/area")
area_refined = area_raw_df.select("area_code", "area_name", "display_level")
area_refined.write.format(refined_format).save(refined_base_path + "area")

# COMMAND ----------

refined_df =spark.read.format(refined_format).load(refined_base_path + "area")
display(refined_df.orderBy("area_code"))


# COMMAND ----------

# Series table
series_raw_df = spark.read.format(raw_format).load(raw_base_path + "/series")
series_refined = series_raw_df.drop("footnote_codes").withColumn("lastRefreshed", lit(load_time)
series_refined.write.mode("overwrite").format(refined_format).save(refined_base_path + "series")

# COMMAND ----------

# Current table
current_raw_df = spark.read.format(raw_format).load(raw_base_path + "/current")
current_refined = current_raw_df.select("*")
current_refined.write.format(refined_format).save(refined_base_path + "/current")
