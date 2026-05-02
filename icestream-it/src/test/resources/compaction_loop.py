import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print(f"compaction loop starting; default catalog={spark.sql('SELECT current_catalog()').collect()}", flush=True)
while True:
    try:
        spark.catalog.refreshTable("iceberg.db.events")
        files = spark.sql("SELECT count(*) AS n FROM iceberg.db.events.files").collect()[0]["n"]
        result = spark.sql(
            "CALL iceberg.system.rewrite_data_files("
            "  table => 'iceberg.db.events', "
            "  options => map('rewrite-all', 'true', 'remove-dangling-deletes', 'true')"
            ")"
        ).collect()
        print(f"compaction iteration succeeded: filesBefore={files} result={result}", flush=True)
    except Exception as exc:
        print(f"compaction iteration failed: {exc}", flush=True)
    time.sleep(2)
