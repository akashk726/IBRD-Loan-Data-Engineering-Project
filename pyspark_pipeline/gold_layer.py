from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from dotenv import load_dotenv

# =========================
# LOAD ENV VARIABLES
# =========================
load_dotenv()

STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

# Paths
silver_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/ibrd_loans/"
gold_path = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

# =========================
# CREATE SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("IBRD Gold Layer - FINAL STAR SCHEMA") \
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6"
    ) \
    .getOrCreate()

# ADLS CONFIG
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

# =========================
# READ SILVER DATA
# =========================
print("Reading Silver Data...")
df = spark.read.parquet(silver_path)

print("Total Rows:", df.count())

# =========================
# CLEAN COLUMN NAMES
# =========================
df = df \
    .withColumnRenamed("country_/_economy", "country") \
    .withColumnRenamed("country_/_economy_code", "country_code") \
    .withColumnRenamed("original_principal_amount_us", "original_principal_amount") \
    .withColumnRenamed("disbursed_amount_us", "disbursed_amount") \
    .withColumnRenamed("repaid_to_ibrd_us", "repaid_to_ibrd") \
    .withColumnRenamed("due_to_ibrd_us", "due_to_ibrd")

df = df.toDF(*[c.lower() for c in df.columns])

# =========================
# DIMENSION TABLES
# =========================

# DIM_COUNTRY
dim_country = df.select("country", "region").dropDuplicates()
dim_country = dim_country.withColumn("country_key", monotonically_increasing_id())

# DIM_BORROWER
dim_borrower = df.select("borrower", "country").dropDuplicates()
dim_borrower = dim_borrower.withColumn("borrower_key", monotonically_increasing_id())

# DIM_LOAN_STATUS
dim_status = df.select("loan_status").dropDuplicates()
dim_status = dim_status.withColumn("status_key", monotonically_increasing_id())

# DIM_YEAR
dim_year = df.select(col("loan_year").alias("year")).dropDuplicates()
dim_year = dim_year.withColumn("year_key", col("year"))

# DIM_LOAN_TYPE
dim_loan_type = df.select("loan_type", "loan_status").dropDuplicates()
dim_loan_type = dim_loan_type.withColumn("loan_type_key", monotonically_increasing_id())

# =========================
# FACT TABLE
# =========================
fact_df = df.select(
    "loan_number",
    "country",
    "borrower",
    "loan_status",
    "loan_type",
    col("loan_year").alias("year"),
    "original_principal_amount",
    "disbursed_amount",
    "repaid_to_ibrd",
    "due_to_ibrd",
    "interest_rate",
    "repayment_ratio",
    "disbursement_ratio"
)

# =========================
# JOIN DIMENSIONS (STAR SCHEMA)
# =========================
fact_df = fact_df \
    .join(dim_country, "country", "left") \
    .join(dim_borrower, ["borrower", "country"], "left") \
    .join(dim_status, "loan_status", "left") \
    .join(dim_year, "year", "left") \
    .join(dim_loan_type, ["loan_type", "loan_status"], "left")

# =========================
# FINAL FACT TABLE
# =========================
fact_df = fact_df.select(
    "loan_number",
    "country_key",
    "borrower_key",
    "status_key",
    "loan_type_key",
    "year_key",
    "original_principal_amount",
    "disbursed_amount",
    "repaid_to_ibrd",
    "due_to_ibrd",
    "interest_rate",
    "repayment_ratio",
    "disbursement_ratio"
)

# =========================
# WRITE GOLD LAYER
# =========================
print("Writing Gold Layer (DIM + FACT)...")

dim_country.write.mode("overwrite").parquet(gold_path + "dim_country/")
dim_borrower.write.mode("overwrite").parquet(gold_path + "dim_borrower/")
dim_status.write.mode("overwrite").parquet(gold_path + "dim_status/")
dim_year.write.mode("overwrite").parquet(gold_path + "dim_year/")
dim_loan_type.write.mode("overwrite").parquet(gold_path + "dim_loan_type/")
fact_df.write.mode("overwrite").parquet(gold_path + "fact_loans/")

print("Gold Layer (STAR SCHEMA) Created Successfully!")

spark.stop()