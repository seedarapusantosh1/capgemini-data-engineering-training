from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,concat_ws,col,count,when

spark = SparkSession.builder.appName("Practice").getOrCreate()

customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
sales=spark.read.format('csv').option('header','true').load('/samples/sales.csv')

#Data Cleaning
##Remove rows with null keys (customer_id etc.)
customers=customers.dropna(subset=["customer_id"])
sales=sales.dropna(subset=["customer_id"])
display(customers)
display(sales)

##Remove duplicates
customers=customers.dropDuplicates()
sales=sales.dropDuplicates()
customers.show()
sales.show()

##Filter invalid values (negative amounts)
sales=sales.filter(sales["total_amount"]>0)
display(sales)

##Check column types
customers.printSchema()
sales.printSchema()

#Task 1: Daily Sales
daily_sales=sales.groupBy(sales["sale_date"].alias("date")).agg(sum("total_amount").alias("total_sales"))
daily_sales.show()

#Task 2: City-wise Revenue
city_wise_revenue=customers.join(sales,"customer_id").groupBy("city").agg(sum("total_amount").alias("Revenue"))
display(city_wise_revenue)

#Task 3: Top 5 Customers
customers=customers.withColumn("name",concat_ws(" ",col("first_name"),col("last_name")))
top_5_customers=customers.join(sales,"customer_id")\
.groupBy("name").agg(\
  sum("total_amount").alias("total_spend"))\
.orderBy(col("total_spend").desc())\
.limit(5)
display(top_5_customers)

#Task 4: Repeat Customers (>1 order)
customer_amount= customers.join(sales, on="customer_id")\
.groupBy("customer_id").agg(\
  count(sales["sale_id"]).alias("total_orders"))\
.filter(col("total_orders")>1)
display(customer_amount)

#Task 5: Customer Segmentation
customer_spend=customers.join(sales,"customer_id").groupBy("name").agg(sum("total_amount").alias("total_spend"))
segment=customer_spend.withColumn("segment",
                                  when(col("total_spend") > 10000, "Gold")
 .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
 .otherwise("Bronze"))
display(segment)

#Task 6: Final Reporting Table
final=customers.join(sales,"customer_id")\
  .withColumn("cus_name",concat_ws(" ",col("first_name"),col("last_name")).alias("cus_name"))\
  .groupBy("cus_name")\
  .agg(sum("total_amount").alias("total_spend"),count("sale_id").alias("order_count"))\
  .withColumn("segment",when(col("total_spend") > 10000, "Gold")\
  .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")\
  .otherwise("Bronze"))
display(final)


#Task 7: Save Output
final.write.mode('overwrite').option("header",True).csv('/samples/output/report')
