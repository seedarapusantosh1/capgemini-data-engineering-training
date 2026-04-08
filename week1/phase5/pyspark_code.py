from pyspark.sql.functions import *
from pyspark.sql.window import Window
print('Setup complete')

display(dbutils.fs.ls('/Volumes/workspace/default/phase5'))
#Loading datasets
orders = spark.read.csv('/Volumes/workspace/default/phase5/olist_orders_dataset.csv', header=True, inferSchema=True).show()
order_items = spark.read.csv('/Volumes/workspace/default/phase5/olist_order_items_dataset.csv', header=True, inferSchema=True).show()
customers = spark.read.csv('/Volumes/workspace/default/phase5/olist_customers_dataset.csv', header=True, inferSchema=True).show()
products = spark.read.csv('/Volumes/workspace/default/phase5/olist_products_dataset.csv', header=True, inferSchema=True).show()
payments = spark.read.csv('/Volumes/workspace/default/phase5/olist_order_payments_dataset.csv', header=True, inferSchema=True).show()

orders.select([count(when(col(c).isNull(), c)).alias(c) for c in orders.columns]).show()
print('Orders:', orders.count())
print('Customers:', customers.count())

orders_customers = orders.join(customers, 'customer_id', 'inner')
display(orders_customers)

fact_orders = order_items.join(orders, 'order_id').join(customers, 'customer_id').join(payments, 'order_id')
display(fact_orders)

#TASKS

## TASK 1: Top 3 Customers per City Calculate total spend per customer, then use window function to rank customers within each city

df = orders.join(customers, "customer_id") \
           .join(payments, "order_id")

customer_spend = df.groupBy("customer_id", "customer_city") \
                   .agg(sum("payment_value").alias("total_spend"))

window_spec = Window.partitionBy("customer_city") \
                    .orderBy(col("total_spend").desc())

ranked = customer_spend.withColumn("rank", dense_rank().over(window_spec))

top3_customers = ranked.filter(col("rank") <= 3)

display(top3_customers)

## TASK 2:Running Total of Sales Calculate daily sales and then cumulative (running) total using window function

df = orders.join(payments, "order_id")

df = df.withColumn("order_date", to_date(col("order_purchase_timestamp")))

daily_sales = df.groupBy("order_date") \
                .agg(sum("payment_value").alias("daily_sales"))

window_spec = Window.orderBy("order_date")

result = daily_sales.withColumn(
    "running_total",
    sum("daily_sales").over(window_spec)
)

display(result)

## TASK 3: Top Products per Category Aggregate sales per product, join with category

payments = payments.withColumn("payment_value", col("payment_value").cast("double"))

payments_per_order = payments.groupBy("order_id") \
    .agg(sum("payment_value").alias("total_payment"))

item_count = order_items.groupBy("order_id") \
    .agg(count("*").alias("item_count"))

order_items_enriched = order_items.join(payments_per_order, "order_id") \
    .join(item_count, "order_id") \
    .withColumn("item_sales", col("total_payment") / col("item_count"))

product_sales = order_items_enriched.groupBy("product_id") \
    .agg(sum("item_sales").alias("total_sales"))

product_with_category = product_sales.join(products, "product_id")

window_spec = Window.partitionBy("product_category_name") \
    .orderBy(col("total_sales").desc())

final = product_with_category.withColumn(
    "rank",
    dense_rank().over(window_spec)
)

final= final.select(
    col("product_category_name").alias("category"),
    "product_id",
    "total_sales",
    "rank"
)

final.show()

## TASK 4: Customer Lifetime Value Calculate total spend per customer across all orders

from pyspark.sql.functions import sum

df = orders.join(payments, "order_id")

lifetime_value= df.groupBy("customer_id") \
        .agg(sum("payment_value").alias("customer_lifetime_value"))

display(lifetime_value)

## TASK 5: Customer Segmentation Apply logic: total_spend > 10000 → Gold 5000–10000 → Silver <5000 → Bronze Add a new column 'segment'. Then group by segment to count customers

df = orders.join(payments, "order_id")

lifetime_value = df.groupBy("customer_id") \
        .agg(sum("payment_value").alias("total_spend"))

segmented = lifetime_value.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)

result = segmented.groupBy("segment") \
                  .agg(count("customer_id").alias("customer_count"))

display(result)

## TASK 6: Final Reporting Table Combine results into a single dataset containing: customer_id, city,total_spend, segment, total_orders

df = orders.join(customers, "customer_id") \
           .join(payments, "order_id")

final = df.groupBy("customer_id", "customer_city") \
             .agg(
                 sum("payment_value").alias("total_spend"),
                 countDistinct("order_id").alias("total_orders")
             )

final = final.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)

final = final.select(
    col("customer_id"),
    col("customer_city").alias("city"),
    col("total_spend"),
    col("segment"),
    col("total_orders")
)

display(final)
