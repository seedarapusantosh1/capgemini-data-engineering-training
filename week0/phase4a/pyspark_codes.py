from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum,concat_ws,col,when,count,ntile

spark = SparkSession.builder.appName("Practice").getOrCreate()

customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
sales=spark.read.format('csv').option('header','true').load('/samples/sales.csv')


# Task 1: Create Gold/Silver/Bronze segmentation using conditional logic
customers=customers.join(sales,"customer_id")\
  .withColumn("customer_name",concat_ws(" ",col("first_name"),col("last_name")))\
  .groupBy("customer_name")\
  .agg(sum("total_amount").alias("total_spend"))\
  .withColumn(
    "Segment",
 when(col("total_spend") > 10000, "Gold")
 .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
 .otherwise("Bronze")
)
display(customers)

# Task 2: Group data by segment and count customers
customers=customers.groupBy("Segment")\
.agg(count("customer_name").alias("customer_name"))
display(customers)

# Task 3: Try quantile-based segmentation
quantiles = customers.approxQuantile("total_spend", [0.33, 0.66], 0)
q1 = quantiles[0]
q2 = quantiles[1]
customers=customers.withColumn(
    "Segment",
 when(col("total_spend") > q2, "Gold")
 .when((col("total_spend") >= q1) & (col("total_spend") <= 10000), "Silver")
 .otherwise("Bronze")
)
display(customers)

#Task 4: Compare results of different methods
ntile = customers.withColumn("tile", ntile(3).over(window_spec)) \
    .withColumn(
        "Segment",
        when(col("tile") == 1, "Gold")
        .when(col("tile") == 2, "Silver")
        .otherwise("Bronze")
    )

ntile_result = ntile.groupBy("Segment") \
    .agg(count("*").alias("ntile_count"))
compare = condition_logic.join(quantile_based).join(ntile_result)
display(compare)
