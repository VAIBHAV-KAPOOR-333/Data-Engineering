from pyspark.sql import SparkSession

# ---------------- SPARK SESSION ----------------

print("🚀 Spark Pipeline Starting...")

spark = SparkSession.builder \
    .appName("Flipkart Spark Pipeline") \
    .getOrCreate()

sc = spark.sparkContext  # ✅ SparkContext

# ---------------- RDD (Concept Coverage) ----------------

print("\n✅ RDD Example")

rdd = sc.textFile("sales_data.csv")

print(rdd.take(5))

# ---------------- DATAFRAME ----------------

print("\n✅ Reading CSV into DataFrame")

df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

df.printSchema()

# ---------------- TRANSFORMATIONS ----------------

print("\n✅ Transformations")

df = df.withColumn("total_amount", df.price * df.quantity)

# SELECT
selected_df = df.select("customer_id", "product", "total_amount")

# FILTER
filtered_df = df.filter(df.total_amount > 50000)

# GROUP BY
product_sales = df.groupBy("product").sum("total_amount")

print("\nTop Product Sales:")
product_sales.show()

# ---------------- READ JSON ----------------

print("\n✅ Reading JSON")

json_df = spark.read.json("sales_data.json")

# ---------------- JOIN ----------------

print("\n✅ Join Example")

joined_df = df.join(json_df, on="customer_id", how="inner")

joined_df.show(5)

# ---------------- READ PARQUET ----------------

print("\n✅ Reading Parquet")

parquet_df = spark.read.parquet("sales_data.parquet")

parquet_df.show(5)

# ---------------- WRITE OUTPUTS ----------------

print("\n✅ Writing Outputs")

df.write.mode("overwrite").parquet("spark_parquet")

df.write.mode("overwrite").json("spark_json")

df.write.mode("overwrite").csv("spark_csv", header=True)

print("\n✅ Spark Pipeline Completed")

spark.stop()
