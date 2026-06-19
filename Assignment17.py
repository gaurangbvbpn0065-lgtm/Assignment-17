# PySpark DataFrame Sales Processing Application

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark Session
spark = SparkSession.builder \
    .appName("SalesDataFrameApplication") \
    .getOrCreate()

# Read CSV file into DataFrame
df = spark.read.csv("sales.csv", header=True, inferSchema=True)

# -------------------------------------------------
# 1. Sort all products by sales in descending order
# -------------------------------------------------

print("\nProducts Sorted by Sales (Descending):")
sorted_df = df.orderBy(col("sales").desc())
sorted_df.show()

# -------------------------------------------------
# 2. Display Top 3 Products with Highest Sales
# -------------------------------------------------

print("\nTop 3 Highest Selling Products:")
top3 = sorted_df.limit(3)
top3.show()

# -------------------------------------------------
# 3. Filter Products with Sales Greater than 80000
#    and Save as CSV
# -------------------------------------------------

high_sales = df.filter(col("sales") > 80000)

high_sales.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/high_sales_products")

print("Filtered data saved successfully in output/high_sales_products")

spark.stop()

# ---------------- sales.csv ----------------
# product_id,product_name,category,sales
# 101,Laptop,Electronics,150000
# 102,Mobile,Electronics,95000
# 103,TV,Electronics,120000
# 104,Chair,Furniture,30000
# 105,Table,Furniture,45000
# 106,Sofa,Furniture,80000
# 107,Headphones,Electronics,25000
# 108,Bed,Furniture,90000

# ---------------- Dockerfile ----------------
# FROM openjdk:11-jre-slim
# RUN apt-get update && apt-get install -y python3 python3-pip wget
# RUN pip3 install pyspark
# WORKDIR /app
# COPY . /app
# CMD ["spark-submit", "app.py"]

# ---------------- README ----------------
# Build:
# docker build -t sales-pyspark .
#
# Run:
# docker run sales-pyspark
#
# Output:
# - Products sorted by sales
# - Top 3 highest selling products
# - Products with sales > 80000 saved as CSV in output/high_sales_products