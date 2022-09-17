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

def create_database(db_name, path, drop=False):
    if drop:
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{path}'")

# create_database("refined", refined_base_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true

# COMMAND ----------


#Area table - SETUP
area_raw_df = spark.read.format(raw_format).load(raw_base_path + "/area_original")
area_refined = area_raw_df.select("area_code", "area_name", "display_level").withColumn("is_deleted", lit(False))
area_refined.write.format(refined_format).mode("overwrite").option("enableChangeDataFeed","true").save(refined_base_path + "/area")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prep source and target tables

# COMMAND ----------

from delta.tables import *

table = 'area'
id = 'area_code'

raw_df = spark.read.format(raw_format).load(raw_base_path + "/area")
df_source = raw_df.select("area_code", "area_name", "display_level").withColumn("is_deleted", lit(False))

delta_target = DeltaTable.forPath(spark, f"{refined_base_path}/{table}")
df_target = delta_target.toDF()

display(df_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete if not in source

# COMMAND ----------

# # DELETE records for testing and demo purposes
# raw_df2 = df_source.filter((col('area_code') != 'S35A') & (col('area_code') != 'S300'))
# raw_df2.write.format(raw_format).mode("overwrite").save(raw_base_path + "/area")

# spark.sql(f'REFRESH TABLE "{raw_base_path}/area"')
# df_source = spark.read.format(raw_format).load(raw_base_path + "/area").select("area_code", "area_name", "display_level").withColumn("is_deleted", lit(False))


# COMMAND ----------

t = df_target.filter("is_deleted == False")
df_source_ids = df_source.select(id)
df_deleted = t.join(df_source_ids, t[id] == df_source_ids[id], "left_anti").withColumn("is_deleted", lit(True))
display(df_deleted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert if any column changed

# COMMAND ----------

# Upsert to delta target table
update_dct = {f"{c}": f"s.{c}" for c in df_target.columns if c != id}
condition_str = ' or '.join(f"t.{k} != {v}" for k,v in update_dct.items())

df_source = df_source.union(df_deleted)

print(condition_str)

delta_target.alias('t') \
.merge(df_source.alias('s'), f"t.{id} = s.{id}") \
.whenMatchedUpdate(condition=f"t.{id} = s.{id} and ({condition_str})", set=update_dct) \
.whenNotMatchedInsertAll() \
.execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge the changes to gold
# MAGIC -- MERGE INTO goldTable t USING silverTable_latest_version s ON s.Country = t.Country
# MAGIC --         WHEN MATCHED AND s._change_type='update_postimage' THEN UPDATE SET VaccinationRate = s.NumVaccinated/s.AvailableDoses
# MAGIC --         WHEN NOT MATCHED THEN INSERT (Country, VaccinationRate) VALUES (s.Country, s.NumVaccinated/s.AvailableDoses)

# COMMAND ----------



# COMMAND ----------

# display(delta_target.history())
refined_df =spark.read.format("delta").option("readChangeData", True).option("startingVersion",1).load(f"{refined_base_path}/{table}")
display(refined_df.orderBy("area_code"))


# COMMAND ----------

# Series table
# series_raw_df = spark.read.format(raw_format).load(raw_base_path + "/series")
# series_refined = series_raw_df.drop("footnote_codes").withColumn("lastRefreshed", lit(load_time)
# series_refined.write.mode("overwrite").format(refined_format).save(refined_base_path + "series")

# COMMAND ----------

# Current table
# current_raw_df = spark.read.format(raw_format).load(raw_base_path + "/current")
# current_refined = current_raw_df.select("*").drop("footnote_codes")
# current_refined.write.mode("overwrite").option("delta.enableChangeDataFeed","true").format(refined_format).saveAsTable("refined_cu.current")

# COMMAND ----------


