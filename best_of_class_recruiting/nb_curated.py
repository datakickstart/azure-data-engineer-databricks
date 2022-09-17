# Databricks notebook source
# MAGIC %run ../utils/mount_storage

# COMMAND ----------

from pyspark.sql.functions import lit, col
from datetime import datetime

load_time = datetime.now()
refined_base_path = dbutils.secrets.get("demo", "refined-datalake-path") + "cu"
curated_base_path = dbutils.secrets.get("demo", "curated-datalake-path") 
refined_format = "delta"
curated_format = "delta"

adls_authenticate()

# COMMAND ----------

def create_database(db_name, path, drop=False):
    if drop:
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE;")    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{path}'")

create_database("refined", refined_base_path)

# COMMAND ----------


#Area table
columns = ["area_code", "area_name", "display_level"]
area_raw_df = spark.read.format(raw_format).load(raw_base_path + "/area")
area_refined = area_raw_df.select(columns)
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

# MAGIC %sql
# MAGIC CREATE DATABASE curated;

# COMMAND ----------

# Fact Current CU table
columns = ["series_id", "year", "period", "value"]
refined_df = spark.read.table("refined_cu.current")
curated_df = refined_df.select(columns)
curated_df.write.mode("overwrite").option("delta.enableChangeDataFeed","true").format(refined_format).saveAsTable("curated.fact_current_cu")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM table_changes('curated.fact_current_cu', 2,10) --where _change_type != 'insert' limit 20
# MAGIC -- 0 - 963989
# MAGIC -- 1 - 1927978
