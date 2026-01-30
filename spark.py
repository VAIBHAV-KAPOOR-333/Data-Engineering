from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstPySparkApp") \
    .getOrCreate()


#Recommended way to create DataFrame (Single tuple not allowed)
data = [
    (1, "Rahul", 25),
    (2, "Amit", 30)
]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.show()


#List of lists
data = [
    [1, "Rahul", 25],
    [2, "Amit", 30]
]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.show()


#The column order is NOT guaranteed when you use dictionaries. (Use select to reorder)
data = [
    {"id": 1, "name": "Rahul", "age": 25},
    {"id": 2, "name": "Amit", "age": 30}
]
df = spark.createDataFrame(data)
df = df.select("id", "name", "age")
df.show()


#Row objects
from pyspark.sql import Row
data = [
    Row(id=1, name="Rahul", age=25),
    Row(id=2, name="Amit", age=30)
]
df = spark.createDataFrame(data)
df.show()


#Pandas DataFrame
import pandas as pd
pdf = pd.DataFrame({
    "id": [1, 2],
    "name": ["Rahul", "Amit"],
    "age": [25, 30]
})
df = spark.createDataFrame(pdf)
df.show()


'''#READING DATA FROM EXTERNAL SOURCES
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/users.csv")
df.show()

df = spark.read.json("data/users.json")
df.show()

df = spark.read.parquet("data/users.parquet")
df.show()'''


#Select, Filter, Columns
df.select("name", "age").show()

df.filter(df.age > 25).show()
df.filter(df["age"] > 25).show()
df.filter("age > 25").show()

from pyspark.sql.functions import col
df.withColumn("age_plus_10", col("age") + 10).show()


#Transformations vs Actions
df2 = df.filter(df["age"] > 25)   # transformation
df2.show()                    # action → triggers execution


'''#GroupBy & Aggregations
df.groupBy("city").count().show()

from pyspark.sql.functions import avg
df.groupBy("city").agg(avg("salary")).show()'''


'''#Write data
df.write \
  .mode("overwrite") \
  .parquet("output/users")
print("Data written successfully in parquet.")

df.write \
  .option("header", "true") \
  .csv("output/users_csv")
print("Data written successfully in csv.")'''


'''#Sample pipeline
df = spark.read.csv("raw/users.csv", header=True, inferSchema=True)
clean_df = df.filter(df.age.isNotNull())
final_df = clean_df.withColumn("age_category",col("age") > 30)
final_df.write.parquet("processed/users")'''





