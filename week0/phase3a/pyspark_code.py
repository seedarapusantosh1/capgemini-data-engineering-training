from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark Session
spark = SparkSession.builder.appName("Phase3A").getOrCreate()

# Create Messy Dataset
data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

customers = spark.createDataFrame(data, columns)

# Show original data
customers.show()

# 1. Identify issues
# Null values
customers.filter(
    col("customer_id").isNull() | 
    col("name").isNull() | 
    col("city").isNull()
).show()

# Duplicate rows
customers.groupBy("customer_id", "name", "city", "age") \
    .count() \
    .filter("count > 1") \
    .show()

# Invalid age
customers.filter(col("age") < 0).show()

# 2. Clean data
# Remove rows with null customer_id
clean_customers = customers.dropna(subset=["customer_id"])

# Fill missing values
clean_customers = clean_customers.fillna({
    "name": "Unknown",
    "city": "Unknown"
})

# Remove duplicates
clean_customers = clean_customers.dropDuplicates()

# Remove invalid age
clean_customers = clean_customers.filter(col("age") >= 0)

# 3. Validate cleaning-
print("Before Cleaning:", customers.count())
print("After Cleaning:", clean_customers.count())

# 4. Aggregation
clean_customers.groupBy("city") \
    .count() \
    .show()
