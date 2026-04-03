**Objectives :**

During this step, I was engaged in completing data transformations with the help of PySpark. The primary aim was to combine datasets, perform aggregations, and derive useful insights out of the data.
____________________________________________________________________________________________________________________________________________________
**Problem Statement :**
The datasets provided to me in this task included such datasets as customers and orders.
The following are:
- The data of various tables combined.
- Computed summative and mean values.
- Selected best customers in terms of spending.
- No orders of identified customers.
- Generated city-wise revenue.
______________________________________________________________________________________________________________________________________________________
**Approach: **
- Entered data into Spark DataFrames.
- Filtered the data with null values in customer id.
- Added orders and customers with customer id.
- Applied transformations like:
- groupBy and aggregations
- filtering conditions
- Generated final outputs
______________________________________________________________________________________________________________________________________________________
**Key Transformations: **
- join() -> to combine customers and orders
- groupBy() -> to group data
- agg() -> to calculate sum, average, count
- filter() -> to apply conditions
- orderBy() -> to sort results
______________________________________________________________________________________________________________________________________________________
** Output:**
Total dollar value of customer.
Top 3 customers in terms of overall spending.
Loyal customers who have no orders.
City-wise total revenue
Mean order value per client.
Customers with more than one order
Ranked customers by aggregate expenditure.
______________________________________________________________________________________________________________________________________________________
** Challenges faced:**
- Understanding join conditions between datasets.
- Handling missing values.
- Converting SQL logic into PySpark.
______________________________________________________________________________________________________________________________________________________
**Learnings: **
- Joining in PySpark.
- Precautions of cleaning data prior to processing.
- The mechanics of aggregations in data of the real world.
- Better SQL to PythonSpark translation.
______________________________________________________________________________________________________________________________________________________
**Files in this Folde:r**
- solution.py -> PySpark implementation
- phase2_problem_statement.pdf -> Problem description
- outputs/ -> Output screenshots
