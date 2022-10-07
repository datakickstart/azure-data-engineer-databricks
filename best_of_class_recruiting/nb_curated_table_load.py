# Databricks notebook source
dbutils.widgets.text("table", "area")
dbutils.widgets.text("id_column", "area_code")
# dbutils.widgets.text("columns","area_code,area_name,display_level")

# COMMAND ----------

# MAGIC %run ../utils/mount_storage

# COMMAND ----------

from pyspark.sql.functions import lit, col
from datetime import datetime

load_time = datetime.now()
refined_base_path = dbutils.secrets.get("demo", "refined-datalake-path") + "cu"
refined_format = "delta"
curated_base_path = dbutils.secrets.get("demo", "curated-datalake-path")
curated_db = 'curated'
curated_format = "delta"
 
adls_authenticate()

# COMMAND ----------

def create_database(db_name, path, drop=False):
    if drop:
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{path}'")

create_database("curated", curated_base_path, False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true

# COMMAND ----------


# #Area table - SETUP
# area_raw_df = spark.read.format(raw_format).load(raw_base_path + "/area_original")
# area_refined = area_raw_df.select("area_code", "area_name", "display_level").withColumn("is_deleted", lit(False))
# area_refined.write.format(refined_format).mode("overwrite").option("enableChangeDataFeed","true").save(refined_base_path + "/area")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prep source and target tables

# COMMAND ----------

from delta.tables import *
from datetime import datetime, timedelta
from pyspark.sql.functions import col, min, max

table = dbutils.widgets.get("table")
id = dbutils.widgets.get("id_column")
# columns = dbutils.widgets.get("columns").split(',')
# if id not in columns:
#     columns = [id] + columns
# print(columns)


delta_source = DeltaTable.forPath(spark, f"{refined_base_path}/{table}")

# refined_df = spark.sql("""
# SELECT * 
#     FROM 
#          (SELECT *, rank() over (partition by Country order by _commit_version desc) as rank
#           FROM table_changes('silverTable', 2, 5)
#           WHERE _change_type !='update_preimage')
#     WHERE rank=1
# """)

last_load = datetime.now() - timedelta(hours=72)
print(last_load.strftime('%Y-%m-%d %H:%M:%S'))

# delta_source.history().printSchema()
version = delta_source.history() #.filter(col("timestamp") > last_load) #.select(min("version"), max("version"))
# display(version)

last_updated = spark.sql(f"select max(last_modified) last_modified from {curated_db}.{table}").first().last_modified + timedelta(hours=0, minutes=0, seconds=1)

refined_df =spark.read.format("delta").option("readChangeFeed", "true") \
            .option("startingTimestamp", last_updated) \
            .load(f"{refined_base_path}/{table}")

#df = spark.read.format(refined_format).load(f"{refined_base_path}/{table}")

# df_source = raw_df.select(*columns).withColumn("is_deleted", lit(False))

display(refined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert if any column changed

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
from delta.tables import *

#     delta_target = DeltaTable.forPath(spark, f"{curated_base_path}/area")
#     display(delta_target.history())
#     refined_df =spark.read.format("delta").option("readChangeData", True).option("startingVersion",1).load(f"{refined_base_path}/{table}")
    display(refined_df.orderBy("area_code"))
    delta_target = DeltaTable.forPath(spark, f"{curated_base_path}/{table}")
#     df_target = delta_target.toDF()

try:
    delta_target = DeltaTable.forPath(spark, f"{curated_base_path}/{table}")

    delta_target.alias('t') \
    .merge(refined_df.alias('s'), f"t.{id} = s.{id}") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
except AnalysisException as e:
    if str(e).find('not a Delta table'):
        print("Table does not exist, need to create table first.")
        
        refined_df =spark.read.format("delta").option("readChangeFeed", "true") \
            .option("startingVersion", 0) \
            .load(f"{refined_base_path}/{table}") \
            .withColumnRenamed("_commit_timestamp", "last_modified") \
            .drop("_commit_version") \
            .drop("_change_type")
        refined_df.write.format("delta").saveAsTable(f"{curated_db}.{table}")


# Upsert to delta target table
# update_dct = {f"{c}": f"s.{c}" for c in df_target.columns if c != id}
# condition_str = ' or '.join(f"t.{k} != {v}" for k,v in update_dct.items())

# df_source = df_source.union(df_deleted)

# print(condition_str)



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

dbutils.notebook.exit("success")
