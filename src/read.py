from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("diag") \
    .master("local[*]") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

# spark.read.parquet("data/fhv_tripdata_2025-01.parquet").show()
# spark.read.parquet("data/green_tripdata_2025-01.parquet").show()

path = "/Users/ivankurchenko/Projects/github/blog-data-quality-on-spark/data/yellow/"
spark.read.parquet(path).show()
