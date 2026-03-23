from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Read values from .env
storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_key = os.getenv("AZURE_STORAGE_KEY")

raw_container = os.getenv("RAW_CONTAINER")
bronze_container = os.getenv("BRONZE_CONTAINER")
raw_file = os.getenv("RAW_FILE")

jars_packages = os.getenv("JARS_PACKAGES")

# Build paths dynamically
raw_path = f"abfss://{raw_container}@{storage_account}.dfs.core.windows.net/{raw_file}"
bronze_path = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/ibrd_loans"

# Create Spark Session
spark = SparkSession.builder \
    .appName("IBRD Bronze Layer") \
    .config("spark.jars.packages", jars_packages) \
    .getOrCreate()

# Windows fix
spark.conf.set("spark.hadoop.io.native.lib.available", "false")
spark.conf.set("spark.hadoop.fs.azure.enable.check.access", "false")

# Set ADLS key securely
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# Read from RAW
df = spark.read.csv(
    raw_path,
    header=True,
    inferSchema=True
)

print("Raw Data Preview:")
df.show(5)

# Write to BRONZE
df = df.repartition(4)

df.write.mode("overwrite").parquet(bronze_path)

print("Bronze layer created securely!")