# Databricks notebook source
def adls_authenticate():
  """
  Dependencies:`
    - Service principal is created and assigned permission
    - Secret scope created using Azure Key Vault or Databricks Secret Scope
    - Key values are added to the secret scope so that references from dbutils.secrets.get work properly
  """
  secret_scope_name = "demo"
  account_name = "dlpssa4dev04"
  directory_id = "9fb75466-59fe-466b-bebb-b5805c51aaba"
  client_id = dbutils.secrets.get(secret_scope_name, 'service-principle-id')
#   app_id = dbutils.secrets.get(secret_scope_name, '')
  credential = dbutils.secrets.get(secret_scope_name, 'service-principle-pwd')
  
  spark.conf.set("fs.azure.account.auth.type", "OAuth")
  spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  spark.conf.set("fs.azure.account.oauth2.client.id", client_id)
  spark.conf.set("fs.azure.account.oauth2.client.secret", credential)
  spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/{0}/oauth2/token".format(directory_id))

# COMMAND ----------

def adls_mount(account_name="dlpssa4dev04", container="raw", mnt_pnt="/mnt/datalake/raw"):
  """
  Dependencies:
    - Service principal is created and assigned permission
    - Secret scope created using Azure Key Vault or Databricks Secret Scope
    - Key values are added to the secret scope so that references from dbutils.secrets.get work properly
  """
  secret_scope_name = "demo"
  account_name = "dlpssa4dev04"
  directory_id = "9fb75466-59fe-466b-bebb-b5805c51aaba"
  client_id = dbutils.secrets.get(secret_scope_name, 'service-principle-id')
  #   app_id = dbutils.secrets.get(secret_scope_name, '')
  credential = dbutils.secrets.get(secret_scope_name, 'service-principle-pwd')

  configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id,
           "fs.azure.account.oauth2.client.secret": credential,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{0}/oauth2/token".format(directory_id)}

  # Optionally, you can add <directory-name> to the source URI of your mount point.
  dbutils.fs.mount(
    source = f"abfss://{container}@{account_name}.dfs.core.windows.net/",
    mount_point = mnt_pnt,
    extra_configs = configs
  )
  

# COMMAND ----------

def default_mounts():
    adls_mount(account_name="dlpssa4dev04", container="raw", mnt_pnt="/mnt/dlpssa/raw")
    adls_mount(account_name="dlpssa4dev04", container="refined", mnt_pnt="/mnt/dlpssa/refined")    
    adls_mount(account_name="dlpssa4dev04", container="curated", mnt_pnt="/mnt/dlpssa/curated")

# COMMAND ----------

# from datetime import datetime

# t = datetime.fromtimestamp(1657812704)
# print(t)
