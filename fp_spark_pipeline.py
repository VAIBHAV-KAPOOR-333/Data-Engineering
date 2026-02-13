from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ---------------- SPARK SESSION ----------------

print("🚀 Spark Pipeline Starting...")

spark = SparkSession.builder \
    .appName("Flipkart Spark Pipeline") \
    .getOrCreate()

sc = spark.sparkContext   # ✅ SparkContext

# Reduce log noise (optional but nice)
sc.setLogLevel("ERROR")

# ---------------- RDD EXAMPLE ----------------

print("\n✅ RDD Example")

rdd = sc.textFile("sales_data.csv")

print(rdd.take(5))   # Action → NEVER assign

# ---------------- DATAFRAME ----------------

print("\n✅ Reading CSV into DataFrame")

df = spark.read.csv(
    "sales_data.csv",
    header=True,
    inferSchema=True
)

df.printSchema()

df.show(5)

# ---------------- TRANSFORMATIONS ----------------

print("\n✅ Adding total_amount Column")

df = df.withColumn(
    "total_amount",
    col("price") * col("quantity")
)

df.show(5)

# ---------------- SELECT ----------------

print("\n✅ Select Example")

selected_df = df.select(
    "customer_id",
    "product",
    "total_amount"
)

selected_df.show(5)

# ---------------- FILTER ----------------

print("\n✅ Filter Example")

filtered_df = df.filter(
    col("total_amount") > 50000
)

filtered_df.show(5)

# ---------------- GROUP BY ----------------

print("\n✅ GroupBy Example")

product_sales = df.groupBy("product") \
    .sum("total_amount")

product_sales.show()

# ---------------- READ JSON ----------------

print("\n✅ Reading JSON File")

json_df = spark.read.json("sales_data.json")

json_df.show(5)

# ---------------- JOIN ----------------

print("\n✅ Join Example")

joined_df = df.join(
    json_df,
    on="customer_id",
    how="inner"
)

joined_df.show(5)

# ---------------- READ PARQUET ----------------

print("\n✅ Reading Parquet File")

parquet_df = spark.read.parquet("sales_data.parquet")

parquet_df.show(5)

# ---------------- WRITE OUTPUTS ----------------

print("\n✅ Writing Output Files")

df.write.mode("overwrite").parquet("spark_parquet")

df.write.mode("overwrite").json("spark_json")

df.write.mode("overwrite").csv("spark_csv", header=True)

print("\n✅ Spark Pipeline Completed Successfully")

spark.stop()
