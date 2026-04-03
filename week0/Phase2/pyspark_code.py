from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,avg,count
spark = SparkSession.builder.appName("Practice").getOrCreate()

customers_data = [
    (1, "John", "Smith", "john.smith@domain.com", "555-0001", "123 Elm St", "Springfield", "IL", "62701"),
    (2, "Emma", "Jones", "emma.jones@webmail.com", "555-0002", "456 Oak St", "Centerville", "OH", "45459"),
    (3, "Olivia", "Brown", "olivia.brown@outlook.com", "555-0003", "789 Pine St", "Greenville", "SC", "29601"),
    (4, "Liam", "Johnson", "liam.johnson@gmail.com", "555-0004", "101 Maple St", "Riverside", "CA", "92501"),
    (5, "Noah", "Williams", "noah.williams@yahoo.com", "555-0005", "202 Birch St", "Lakeside", "TX", "75001")
]

sales_data = [
    (1, 1, 1, "2024-01-15", 2, 39.98),
    (2, 1, 3, "2024-01-20", 1, 29.99),
    (3, 2, 2, "2024-01-16", 1, 25.00),
    (4, 2, 4, "2024-01-22", 3, 89.97),
    (5, 3, 5, "2024-01-17", 2, 49.98)
]

customers = spark.createDataFrame(customers_data, [
    "customer_id", "first_name", "last_name", "email",
    "phone_number", "address", "city", "state", "zip_code"
])

sales = spark.createDataFrame(sales_data, [
    "sale_id", "customer_id", "product_id",
    "sale_date", "quantity", "total_amount"
])

customers_data = [
    (1, "John", "Smith", "john.smith@domain.com", "555-0001", "123 Elm St", "Springfield", "IL", "62701"),
    (2, "Emma", "Jones", "emma.jones@webmail.com", "555-0002", "456 Oak St", "Centerville", "OH", "45459"),
    (3, "Olivia", "Brown", "olivia.brown@outlook.com", "555-0003", "789 Pine St", "Greenville", "SC", "29601"),
    (4, "Liam", "Johnson", "liam.johnson@gmail.com", "555-0004", "101 Maple St", "Riverside", "CA", "92501"),
    (5, "Noah", "Williams", "noah.williams@yahoo.com", "555-0005", "202 Birch St", "Lakeside", "TX", "75001")
]

sales_data = [
    (1, 1, 1, "2024-01-15", 2, 39.98),
    (2, 1, 3, "2024-01-20", 1, 29.99),
    (3, 2, 2, "2024-01-16", 1, 25.00),
    (4, 2, 4, "2024-01-22", 3, 89.97),
    (5, 3, 5, "2024-01-17", 2, 49.98)
]

customers = spark.createDataFrame(customers_data, ["customer_id", "first_name", "last_name", "email","phone_number", "address", "city", "state", "zip_code"])

sales = spark.createDataFrame(sales_data, ["sale_id", "customer_id", "product_id","sale_date", "quantity", "total_amount"])

# codes
    - Total order amount for each customer
               customers.join(sales, "customer_id").groupBy("customer_id", "first_name").sum("total_amount").show()
                        
    - Top 3 customers by total spend 
            customers.join(sales, "customer_id").groupBy("customer_id", "first_name").sum("total_amount") .orderBy(col("sum(total_amount)").desc()).show(3)

    - Customers with no orders
            customers.join(sales, "customer_id", "left").filter(sales.customer_id.isNull()).show()

    - City-wise total revenue
            customers.join(sales, "customer_id").groupBy("city").agg(sum("total_amount").alias("total_revenue")).show()

    - Average order amount per customer
          sales.groupBy("customer_id").agg(avg("total_amount").alias("avg_amount")).show()

    -  Customers with more than one order
           sales.groupBy("customer_id").agg(count("sale_id").alias("order_count")).filter("order_count > 1").show()

    -  Sort customers by total spend descending
            customers.join(sales, "customer_id").groupBy("first_name").agg(sum("total_amount").alias("total_spend")).orderBy(col("total_spend").desc()).show()
