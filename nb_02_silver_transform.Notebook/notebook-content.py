# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC { 
# MAGIC             "defaultLakehouse": {
# MAGIC                 "name": 'lh_olist',
# MAGIC                 "id": '0f7e660c-8270-45b9-8490-086eba9a7f9d',
# MAGIC                 "workspaceId": '70d2278e-9339-4ad3-b727-8e1d0fbb35ca'
# MAGIC             }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# parameters
run_date = ""
source_prefix = "bronze_"
target_prefix = "silver_"

# import Python packages
import uuid
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# logging
# generates a unique id to be passed through and be used as reference run id
run_id = str(uuid.uuid4())
# generates run date to be referenced in logging
run_date = run_date or date.today().isoformat()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# source-target dictionary
SOURCES = {
    "category_translation":				"category_translation",
    "products":							"products",
    "sellers":							"sellers",
    "customers":						"customers",
    "geolocation":         				"geolocation",
    "orders":              				"orders",
    "order_items":              		"order_items",
    "payments":							"payments",
    "reviews":							"reviews",
}


# check logging details
print(f"run_id={run_id} run_date={run_date} sources={len(SOURCES)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# define functions
def load_silver(src: str, tgt: str) -> int:
    """Read one table as received (all strings) and overwrite silver_<name>. Returns the row count."""

    # load raw files to table
    df = (spark.read.table(f"{source_prefix}{src}")).withColumnRenamed("_run_id", "_bronze_run_id")
    
    # clean up column names
    df = df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])

    # add metadata columns
    df = (df.withColumn("_silver_run_id", F.lit(run_id))
            .withColumn("_processed_at", F.current_timestamp())   
    )

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))

    # get table name
    table = f"{target_prefix}{tgt}"

    # save as table
    (df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(table)
    )

    # return data count inserted in table 
    return spark.table(table).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# load each tables
results, failures = [], []

# loop each table indicated in SOURCES
for src, tgt in SOURCES.items():
    # try attempt, for error handling
    try:
        # load table by using function defined above
        n = load_silver(src, tgt)

        # add value of table name to tuple defined above
        results.append((f"{source_prefix}{src}", f"{target_prefix}{tgt}", n))

        print(f"OK  {target_prefix}{tgt:<22}{n:>12,} rows")

    # error handling block
    except Exception as e:
        # add failed tables to tuple defined above
        failures.append((tgt, str(e)[:500]))

        print(f"FAIL    {target_prefix}{tgt}: {str(e)[:200]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# audit logging
rows = ([(run_id, run_date, t, f, n, "succeeded") for t, f, n in results] +
        [(run_id, run_date, f"{target_prefix}{name}", None, None, "failed") for name, _ in failures])

audit_df = spark.createDataFrame(
    rows,
    "run_id string, run_date string, table_name string, source_file string, row_count long, status string"
).withColumn("logged_at", F.current_timestamp())

# save logs to table audit_row_counts
audit_df.write.format("delta").mode("append").saveAsTable("audit_row_counts")

display(audit_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fail the run if any table fails
if failures:
    for name, err in failures:
        print(f"{name}: {err}")

    raise RuntimeError(f"Silver load failed for: {', '.join(n for n, _ in failures)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# exit session
import notebookutils
import json

# Send status of notebook
notebookutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
