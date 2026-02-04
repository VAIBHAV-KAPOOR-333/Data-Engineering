from pyspark.sql import SparkSession
import random
import os

# ============================================================
# 1. Create Spark Session
# ============================================================

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SalesDataGenerator") \
    .getOrCreate()

# ============================================================
# Helper Function: Rename Spark CSV Output
# Spark always creates part-*.csv inside a folder
# This function renames it to a meaningful file name
# ============================================================

def rename_csv(output_dir, final_name):
    for file in os.listdir(output_dir):
        if file.startswith("part-") and file.endswith(".csv"):
            os.rename(
                os.path.join(output_dir, file),
                os.path.join(output_dir, final_name)
            )

# ============================================================
# 2. CUSTOMER TABLE (1000 records)
# ============================================================

first_names = [
    "Amit", "Rahul", "Neha", "Priya", "Ankit",
    "Pooja", "Suresh", "Ravi", "Kiran", "Sneha"
]

last_names = [
    "Sharma", "Verma", "Patel", "Singh",
    "Kumar", "Gupta", "Mehta", "Joshi"
]

cities = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai"]
country = "India"

customers = []

for i in range(1, 1001):
    first = random.choice(first_names)
    last = random.choice(last_names)

    customers.append((
        i,                                      # customer_id
        f"{first} {last}",                      # customer_name
        f"{first.lower()}{i}@gmail.com",        # email
        random.choice(cities),                  # city
        country,                                # country
        f"2024-01-{random.randint(1,28)}"       # created_date
    ))

customer_df = spark.createDataFrame(
    customers,
    ["customer_id", "customer_name", "email", "city", "country", "created_date"]
)

customer_df.show(5)

# ============================================================
# 3. PRODUCT TABLE (200 records)
# ============================================================

product_catalog = {
    "Electronics": ["Mobile", "Laptop", "Headphones", "Tablet", "Smart Watch"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Shirt", "Sweater"],
    "Grocery": ["Rice", "Wheat", "Sugar", "Oil", "Milk"],
    "Furniture": ["Chair", "Table", "Sofa", "Bed", "Cupboard"]
}

products = []
product_id = 1

for category, items in product_catalog.items():
    for item in items:
        for _ in range(10):   # multiple products per category
            products.append((
                product_id,                       # product_id
                f"{item} {product_id}",           # product_name
                category,                         # category
                round(random.uniform(100, 5000), 2)  # price
            ))
            product_id += 1

product_df = spark.createDataFrame(
    products,
    ["product_id", "product_name", "category", "price"]
)

product_df.show(5)

# ============================================================
# 4. ORDERS TABLE (5000 records)
# ============================================================

orders = []

for i in range(1, 5001):
    product = random.choice(products)
    quantity = random.randint(1, 5)

    orders.append((
        i,                                      # order_id
        random.randint(1, 1000),                # customer_id
        product[0],                             # product_id
        quantity,                               # quantity
        f"2024-02-{random.randint(1,28)}",      # order_date
        round(product[3] * quantity, 2)         # total_amount
    ))

orders_df = spark.createDataFrame(
    orders,
    ["order_id", "customer_id", "product_id", "quantity", "order_date", "total_amount"]
)

orders_df.show(5)

# ============================================================
# 5. SAVE CSV FILES IN CURRENT DIRECTORY
# ============================================================

current_dir = os.getcwd()
base_path = f"{current_dir}/snowflake_raw_data"

customer_path = f"{base_path}/customer"
product_path = f"{base_path}/product"
orders_path = f"{base_path}/orders"

# Write CSVs (Spark output)
customer_df.coalesce(1).write.mode("overwrite").option("header", True).csv(customer_path)
product_df.coalesce(1).write.mode("overwrite").option("header", True).csv(product_path)
orders_df.coalesce(1).write.mode("overwrite").option("header", True).csv(orders_path)

# Rename files to meaningful names
rename_csv(customer_path, "customer.csv")
rename_csv(product_path, "product.csv")
rename_csv(orders_path, "orders.csv")

print(f"CSV files successfully saved at:\n{base_path}")

# ============================================================
# 6. Stop Spark Session
# ============================================================

spark.stop()
