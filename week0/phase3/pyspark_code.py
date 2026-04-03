from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,col,concat_ws,count

spark = SparkSession.builder.appName("Practice").getOrCreate()

customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
sales=spark.read.format('csv').option('header','true').load('/samples/sales.csv')

Codes:

1. Read sales data -> clean nulls -> calculate daily sales
    sales.groupBy("sale_date").agg(sum("total_amount").alias("daily_sales")).show()

2. Read customer data -> clean invalid rows -> city-wise revenue
  customers.join(sales, "customer_id").groupBy("city").agg(sum("total_amount").alias("total_revenue")).show()

3. Find repeat customers (>2 orders)
 customers.join(sales, "customer_id").groupBy("city").agg(sum("total_amount").alias("total_revenue")).show()

4. Find highest spending customer in each city
  customer_totals = customers.join(sales, "customer_id")\
  .withColumn("total_amount", col("total_amount"))\
  .withColumn("cus_name", concat_ws(" ", col("first_name"), col("last_name")).alias("cus_name"))\
  .groupBy( "cus_name", "city") \
  .agg(sum("total_amount").alias("amount")) 
  display(customer_totals)

5. Build final reporting table with customer, city, total spend, order count
  final=customers.join(sales,"customer_id")\
  .withColumn("cus_name",concat_ws(" ",col("first_name"),col("last_name")).alias("cus_name"))\
  .groupBy("cus_name")\
  .agg(sum("total_amount").alias("total_spend"),count("sale_id").alias("order_count"))
  display(final)
