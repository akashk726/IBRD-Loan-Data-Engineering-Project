from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, year, month, round
from dotenv import load_dotenv
import os

# ==============================
# 1. Load Environment Variables
# ==============================
load_dotenv()

storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
storage_key = os.getenv("AZURE_STORAGE_KEY")

# Debug (IMPORTANT)
print("Storage Account:", storage_account)

# ==============================
# 2. Define Paths
# ==============================
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/ibrd_loans/"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/ibrd_loans/"

print("Bronze Path:", bronze_path)
print("Silver Path:", silver_path)

# ==============================
# 3. Create Spark Session
# ==============================
spark = SparkSession.builder \
    .appName("IBRD Silver Layer") \
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6"
    ) \
    .getOrCreate()

# Set ADLS Key
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# Windows fix
spark.conf.set("spark.hadoop.io.native.lib.available", "false")

# ==============================
# 4. Read Bronze Data
# ==============================
print("Reading Bronze Data...")
df = spark.read.parquet(bronze_path)

print("Row Count:", df.count())
df.show(5)

# ==============================
# 5. Remove Duplicates
# ==============================
df = df.dropDuplicates()

# ==============================
# 6. Handle Null Values
# ==============================
df = df.fillna({
    "Currency of Commitment": "UNKNOWN",
    "Guarantor": "UNKNOWN",
    "Loan Status": "UNKNOWN"
})

# ==============================
# 7. Convert Date Columns
# ==============================
date_columns = [
    "End of Period",
    "First Repayment Date",
    "Last Repayment Date",
    "Agreement Signing Date",
    "Board Approval Date",
    "Effective Date (Most Recent)",
    "Closed Date (Most Recent)",
    "Last Disbursement Date"
]

for c in date_columns:
    df = df.withColumn(c, to_date(col(c), "MM/dd/yyyy"))

# ==============================
# 8. Convert Numeric Columns
# ==============================
numeric_columns = [
    "Original Principal Amount (US$)",
    "Cancelled Amount (US$)",
    "Undisbursed Amount (US$)",
    "Disbursed Amount (US$)",
    "Repaid to IBRD (US$)",
    "Due to IBRD (US$)",
    "Exchange Adjustment (US$)",
    "Borrower's Obligation (US$)",
    "Sold 3rd Party (US$)",
    "Repaid 3rd Party (US$)",
    "Due 3rd Party (US$)",
    "Loans Held (US$)",
    "Interest Rate"
]

for c in numeric_columns:
    df = df.withColumn(c, col(c).cast("double"))

# ==============================
# 9. Feature Engineering 
# ==============================

# Fully paid flag
df = df.withColumn(
    "is_fully_paid",
    when(col("Due to IBRD (US$)") == 0, 1).otherwise(0)
)

# Disbursement ratio
df = df.withColumn(
    "disbursement_ratio",
    round(col("Disbursed Amount (US$)") / col("Original Principal Amount (US$)"), 2)
)

# Repayment ratio
df = df.withColumn(
    "repayment_ratio",
    round(col("Repaid to IBRD (US$)") / col("Disbursed Amount (US$)"), 2)
)

# Loan year
df = df.withColumn(
    "loan_year",
    year(col("Agreement Signing Date"))
)

# Year-month for trend
df = df.withColumn(
    "year_month",
    (year(col("End of Period")) * 100 + month(col("End of Period")))
)

# ==============================
# 10. Comparisons
# ==============================

# Disbursed vs Original
df = df.withColumn(
    "disbursed_vs_original",
    col("Disbursed Amount (US$)") - col("Original Principal Amount (US$)")
)

# Repaid vs Disbursed
df = df.withColumn(
    "repaid_vs_disbursed",
    col("Repaid to IBRD (US$)") - col("Disbursed Amount (US$)")
)

# Outstanding amount
df = df.withColumn(
    "outstanding_amount",
    col("Disbursed Amount (US$)") - col("Repaid to IBRD (US$)")
)

# Risk category
df = df.withColumn(
    "risk_category",
    when(col("outstanding_amount") > 100000000, "HIGH")
    .when(col("outstanding_amount") > 10000000, "MEDIUM")
    .otherwise("LOW")
)

# ==============================
# 11. Clean Column Names
# ==============================
for column in df.columns:
    new_col = column.lower() \
        .replace(" ", "_") \
        .replace("(", "") \
        .replace(")", "") \
        .replace("$", "")
    df = df.withColumnRenamed(column, new_col)

# ==============================
# 12. Repartition (Performance)
# ==============================
df = df.repartition(4)

# ==============================
# 13. Write to Silver Layer
# ==============================
print("Writing to Silver Layer...")

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(silver_path)

print("Silver Layer Created Successfully!")

# ==============================
# 14. Stop Spark
# ==============================
spark.stop()