# ---------- Config ----------
account = "stgfinedgedatalake"
container = "datalake"
lake = f"abfss://{container}@{account}.dfs.core.windows.net"

bronze_customers = f"{lake}/bronze/customers"
bronze_accounts  = f"{lake}/bronze/accounts"
bronze_txns      = f"{lake}/bronze/transactions"

silver_root = f"{lake}/silver"
gold_root   = f"{lake}/gold"

#################

from pyspark.sql.types import *
from pyspark.sql.functions import col, trim, year, month

# ---------- Schemas ----------
customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("annual_income", IntegerType(), True),
    StructField("credit_score", IntegerType(), True)
])

accounts_schema = StructType([
    StructField("account_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("account_type", StringType(), True),
    StructField("balance", IntegerType(), True)
])

txns_schema = StructType([
    StructField("txn_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("amount", IntegerType(), True),
    StructField("txn_type", StringType(), True),
    StructField("txn_date", TimestampType(), True)
])

# ---------- Paths ----------
account = "stgfinedgedatalake"
container = "datalake"
lake = f"abfss://{container}@{account}.dfs.core.windows.net"

bronze_customers = f"{lake}/bronze/customers"
bronze_accounts  = f"{lake}/bronze/accounts"
bronze_txns      = f"{lake}/bronze/transactions"

# ---------- Read CSVs ----------
customers_bz = (
    spark.read
        .option("header", True)
        .schema(customers_schema)
        .csv(bronze_customers)
)

accounts_bz = (
    spark.read
        .option("header", True)
        .schema(accounts_schema)
        .csv(bronze_accounts)
)

txns_bz = (
    spark.read
        .option("header", True)
        .schema(txns_schema)
        .csv(bronze_txns)
)

# Add year/month for partitioning
txns_bz = (
    txns_bz
        .withColumn("txn_year", year(col("txn_date")))
        .withColumn("txn_month", month(col("txn_date")))
)

display(customers_bz.limit(5))
display(accounts_bz.limit(5))
display(txns_bz.limit(5))

    ###############################

    # Customers & Accounts (no partitions; small dimension-ish)
customers_bz.write.mode("overwrite").parquet(f"{lake}/bronze_parquet/customers")
accounts_bz.write.mode("overwrite").parquet(f"{lake}/bronze_parquet/accounts")

# Transactions partitioned by year/month to match their date range (Nov 2025 → Feb 2026)
txns_bz.write.mode("overwrite") \
    .partitionBy("txn_year","txn_month") \
    .parquet(f"{lake}/bronze_parquet/transactions")

    ###########################################

    from pyspark.sql.functions import when, sum as Fsum, count as Fcount, avg as Favg, \
                                   countDistinct, upper, lit, col

# Read Bronze Parquet
customers_sv = spark.read.parquet(f"{lake}/bronze_parquet/customers").dropDuplicates(["customer_id"])
accounts_sv  = spark.read.parquet(f"{lake}/bronze_parquet/accounts").dropDuplicates(["account_id"])
txns_sv      = spark.read.parquet(f"{lake}/bronze_parquet/transactions").dropDuplicates(["txn_id"])

# Basic hygiene
customers_sv = (customers_sv
    .withColumn("city", trim(col("city")))
    .withColumn("credit_score", col("credit_score").cast("int"))
    .na.fill({"city": "Unknown"})
)

accounts_sv = accounts_sv.na.fill({"account_type":"Unknown","balance":0})

# Derived fields on transactions
txns_sv = (txns_sv
    .withColumn("is_high_value", when(col("amount") >= 150000, 1).otherwise(0))
    .withColumn("is_debit", when(upper(col("txn_type"))=="DEBIT", 1).otherwise(0))
    .withColumn("is_credit", when(upper(col("txn_type"))=="CREDIT",1).otherwise(0))
)

# Aggregate recent activity per customer (month-level)
cust_monthly = (txns_sv.groupBy("customer_id","txn_year","txn_month")
    .agg(
        Fsum(when(col("is_debit")==1, col("amount")).otherwise(lit(0))).alias("debit_outflow"),
        Fsum(when(col("is_credit")==1, col("amount")).otherwise(lit(0))).alias("credit_inflow"),
        Fsum("is_high_value").alias("high_value_txn_cnt"),
        Fcount("*").alias("txn_cnt")
    )
)

# Current account snapshot (use latest balance per customer via max balance across accounts)
from pyspark.sql.window import Window
w_cust = Window.partitionBy("customer_id")
acct_by_customer = (accounts_sv
    .groupBy("customer_id")
    .agg(Fsum("balance").alias("total_balance"),
         countDistinct("account_type").alias("product_count"))
)

# Join with customers to compute a proxy risk_score
from pyspark.sql.functions import coalesce

silver_customer_monthly = (cust_monthly
    .join(customers_sv, "customer_id", "left")
    .join(acct_by_customer, "customer_id", "left")
    .na.fill({"total_balance":0,"product_count":0})
)

# Proxy risk score: higher with low credit_score, frequent high-value debits, low/negative balance
silver_customer_monthly = (silver_customer_monthly
    .withColumn("risk_score",
        (when(col("credit_score") < 600, 2).otherwise(0) +
         when(col("high_value_txn_cnt") >= 3, 3).otherwise(0) +
         when(col("total_balance") < 0, 3).when(col("total_balance") < 50000, 1).otherwise(0) +
         when(col("debit_outflow") > col("credit_inflow")*1.5, 2).otherwise(0)
        ).cast("int")
     )
)

# Persist Silver
silver_customer_monthly.write.mode("overwrite") \
    .partitionBy("txn_year","txn_month") \
    .parquet(f"{silver_root}/customer_monthly")

customers_sv.write.mode("overwrite").parquet(f"{silver_root}/customers")
accounts_sv.write.mode("overwrite").parquet(f"{silver_root}/accounts")
txns_sv.write.mode("overwrite").partitionBy("txn_year","txn_month").parquet(f"{silver_root}/transactions")

################

from pyspark.sql.functions import countDistinct

monthly_active = (txns_sv
    .groupBy("txn_year","txn_month")
    .agg(countDistinct("customer_id").alias("monthly_active_customers"))
)

monthly_active.write.mode("overwrite").parquet(f"{gold_root}/monthly_active")

##################

customer_risk = (silver_customer_monthly
    .groupBy("txn_year","txn_month","customer_id","city","credit_score")
    .agg(
        Fsum("debit_outflow").alias("debit_outflow"),
        Fsum("credit_inflow").alias("credit_inflow"),
        Favg("risk_score").alias("avg_risk_score"),
        Fsum("high_value_txn_cnt").alias("high_value_txn_cnt"),
        Favg("total_balance").alias("avg_total_balance")
    )
)

customer_risk.write.mode("overwrite").partitionBy("txn_year","txn_month").parquet(f"{gold_root}/customer_risk")

###########################################


# Map each txn to account_type via customer_id
from pyspark.sql.functions import first

cust_to_product = (accounts_sv
    .groupBy("customer_id")
    .agg(first("account_type").alias("account_type")))

txns_with_prod = txns_sv.join(cust_to_product, "customer_id", "left")

product_perf = (txns_with_prod.groupBy("txn_year","txn_month","account_type")
    .agg(
        Fsum(when(col("is_credit")==1, col("amount")).otherwise(lit(0))).alias("total_inflow"),
        Fsum(when(col("is_debit")==1, col("amount")).otherwise(lit(0))).alias("total_outflow")
    )
    .withColumn("profitability_proxy", col("total_inflow") - col("total_outflow"))
)

product_perf.write.mode("overwrite").partitionBy("txn_year","txn_month").parquet(f"{gold_root}/product_performance")


###########################

from pyspark.sql.functions import window

debits = txns_sv.filter((col("is_debit")==1) & (col("is_high_value")==1))

# Window of 60 minutes; produce counts per (customer, window)
suspicious_windows = (debits
    .groupBy("customer_id", window("txn_date", "60 minutes"))
    .agg(Fcount("*").alias("hi_val_cnt"))
    .filter(col("hi_val_cnt") >= 3)
)

# Summarize per month
from pyspark.sql.functions import to_timestamp

fraud_summary = (suspicious_windows
    .withColumn("txn_year", year(col("window.start")))
    .withColumn("txn_month", month(col("window.start")))
    .groupBy("txn_year","txn_month","customer_id")
    .agg(Fcount("*").alias("suspicious_window_cnt"))
)

fraud_summary.write.mode("overwrite").partitionBy("txn_year","txn_month").parquet(f"{gold_root}/fraud_summary")



################################

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

w = Window.partitionBy("txn_year", "txn_month") \
          .orderBy(
              F.col("avg_risk_score").desc(),
              F.col("high_value_txn_cnt").desc(),
              F.col("debit_outflow").desc()
          )

top100_risky = (
    customer_risk
        .withColumn("risk_rank", row_number().over(w))
        .filter(F.col("risk_rank") <= 100)
)

top100_risky.write.mode("overwrite") \
    .partitionBy("txn_year", "txn_month") \
    .parquet(f"{gold_root}/top100_risky")


######################


