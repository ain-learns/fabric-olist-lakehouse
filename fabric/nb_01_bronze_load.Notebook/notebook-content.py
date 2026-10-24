# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Parameters
source_folder = "Files/bronze/olist"
run_date = ""

# import Python packages
import uuid
from datetime import date
from pyspark.sql import functions as F

# logging
# generates a unique id to be passed through and be used as reference run id
run_id = str(uuid.uuid4())
# generates run date to be referenced in logging
run_date = run_date or date.today().isoformat()

# source-target dictionary
SOURCES = {
    "orders":                       "olist_orders_dataset",
    "order_items":                  "olist_order_items_dataset",
    "customers":                    "olist_customers_dataset",
    "products":                     "olist_products_dataset",
    "sellers":                      "olist_sellers_dataset",
    "payments":                     "olist_order_payments_dataset",
    "reviews":                      "olist_order_reviews_dataset",
    "geolocation":                  "olist_geolocation_dataset",
    "category_translation":         "product_category_name_translation",
}

# check logging details
print(f"run_id={run_id} run_date={run_date} sources={len(SOURCES)}")

# define functions
def load_bronze(name: str, file_stem: str) -> int:
    """Read one CSV as received (all strings) and overwrite bronze_<name>. Returns the row count."""

    # get file path
    path = f"{source_folder}/{file_stem}.csv"

    # load raw files to table
    df = (spark.read
            .option("header", True)
            .option("multiline", True)
            .option("quote", '"')
            .option("escape", '"')
            .csv(path)
            )

    # clean up column names
    df = df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])

    # add logging fields
    df = (df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(f"{file_stem}.csv"))
            .withColumn("_run_id", F.lit(run_id))
    )

    # get table name
    table = f"bronze_{name}"

    # save as table
    (df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )

    # return data count inserted in table 
    return spark.table(table).count()

# load each tables
results, failures = [], []

# loop each table indicated in SOURCES
for name, stem in SOURCES.items():
    # try attempt, for error handling
    try:
        # load table by using function defined above
        n = load_bronze(name, stem)

        # add value of table name to tuple defined above
        results.append((f"bronze_{name}", f"{stem}.csv", n))

        print(f"OK  bronze_{name:<22}{n:>12,} rows")

    # error handling block
    except Exception as e:
        # add failed tables to tuple defined above
        failures.append((name, str(e)[:500]))

        print(f"FAIL    bronze_{name}: {str(e)[:200]}")

# audit logging
rows = ([(run_id, run_date, t, f, n, "succeeded") for t, f, n in results] +
        [(run_id, run_date, f"bronze_{name}", None, None, "failed") for name, _ in failures])

audit_df = spark.createDataFrame(
    rows,
    "run_id string, run_date string, table_name string, source_file string, row_count long, status string"
).withColumn("logged_at", F.current_timestamp())

# save logs to table audit_row_counts
audit_df.write.format("delta").mode("append").saveAsTable("audit_row_counts")

display(audit_df)

# fail the run if any table fails
if failures:
    for name, err in failures:
        print(f"{name}: {err}")

    raise RuntimeError(f"Bronze load failed for: {', '.join(n for n, _ in failures)}")

# if nothing failed, proceeed
print(f"Bronze load complete: {len(results)} tables loaded.")

display(spark.sql("SELECT * FROM bronze_orders LIMIT 5"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
