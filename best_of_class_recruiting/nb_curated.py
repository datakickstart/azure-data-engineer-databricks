# Databricks notebook source
# MAGIC %run ../utils/mount_storage

# COMMAND ----------

from pyspark.sql.functions import lit, col
from datetime import datetime

load_time = datetime.now()
refined_base_path = dbutils.secrets.get("demo", "refined-datalake-path") + "cu"
curated_base_path = dbutils.secrets.get("demo", "refined-datalake-path") + "cu_curated"
refined_format = "delta"
curated_format = "delta"

adls_authenticate()

# COMMAND ----------

def load_table(args):
    status = dbutils.notebook.run("nb_curated_table_load", 60, arguments=args)
    print(status)
    if status != 'success':
        raise Exception(f"Failed to load curated database. Status: {str(status)}")

table_list = [
    {"table": "area", "destination_table": "dim_area", "id_column": "area_code" },
    {"table": "series", "table": "dim_series", "id_column": "area_code" },
    {"table": "current", "table": "fact_current", "id_column": "series_id,year,period" },
    {"table": "item", "table": "dim_item", "id_column": "item_code"}
]

# COMMAND ----------

from threading import Thread
from queue import Queue

q = Queue()

worker_count = 2

def run_tasks(function, q):
    while not q.empty():
        try:
            value = q.get()
            function(value)
        except Exception as e:
            table = value.get("table", "UNKNOWN TABLE")
            print(f"Error processing table {table}")
            print(e)
        finally:
            q.task_done()


print(table_list)

for table_args in table_list:
    q.put(table_args)

for i in range(worker_count):
    t=Thread(target=run_tasks, args=(load_table, q))
    t.daemon = True
    t.start()

q.join()


# COMMAND ----------

# # Fact Current CU table
# columns = ["series_id", "year", "period", "value"]
# refined_df = spark.read.table("refined_cu.current")
# curated_df = refined_df.select(columns)
# curated_df.write.mode("overwrite").option("delta.enableChangeDataFeed","true").format(refined_format).saveAsTable("curated.fact_current_cu")

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
from delta.tables import *
try:
    delta_target = DeltaTable.forPath(spark, f"{curated_base_path}/area")
    display(delta_target.history())
    refined_df =spark.read.format("delta").option("readChangeData", True).option("startingVersion",1).load(f"{refined_base_path}/{table}")
    display(refined_df.orderBy("area_code"))
except AnalysisException as e:
    if str(e).find('not a Delta table'):
        print("Table does not exist, need to create table first.")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT count(1) FROM table_changes('curated.fact_current_cu', 2,10) --where _change_type != 'insert' limit 20
# MAGIC -- 0 - 963989
# MAGIC -- 1 - 1927978

# COMMAND ----------


