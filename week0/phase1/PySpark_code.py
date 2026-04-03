from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Phase1").getOrCreate()

customers = spark.createDataFrame([
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28)
], ["customer_id", "customer_name", "city", "age"])

#Implementing the codes
- Show all the customers**
    customers.show()
  
- Show customers  from Chennai
  customers.filter("city = 'Chennai'").show()

- Show customers with age> 25
  customers.filter(customers.age > 25).show()

- Show inly customer_name and city
   customers.select("customer_name", "city").show()

- count customers city-wise
   customers.groupBy("city").count().show()
