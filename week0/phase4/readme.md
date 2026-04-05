
 **Objective:**

In this phase, I built a complete data pipeline using PySpark. I worked on combining data, cleaning it, and generating useful business insights.
___________________________________________________________________________________________________________________________________________________

 **Problem Summary:**

  In this project, I performed different tasks like:

* Calculating daily sales
* Finding revenue for each city
* Identifying top customers
* Finding repeat customers
* Creating customer segments (Gold, Silver, Bronze)
* Building a final report table
______________________________________________________________________________________________________________________________________________________

 **Approach:**
* First, I loaded the datasets into PySpark
* Cleaned the data by removing null values and duplicates
* Filtered invalid data like negative values
* Joined customer and sales data
* Applied transformations and aggregations
* Built a final dataset with all insights
______________________________________________________________________________________________________________________________________________________

**Key Functions Used:**
* **createDataFrame()**-> to create dataset
* **join()** -> to combine tables
* **groupBy()** -> to group data
* **sum()** -> to calculate total values
* **count()** -> to count records
* **filter()** -> to remove invalid data
* **withColumn()** -> to create new columns
* **orderBy()** -> to sort data
* **write()** -> to save output
______________________________________________________________________________________________________________________________________________________
 
 **Outputs:**
* Daily sales report
* City-wise revenue
* Top 5 customers
* Repeat customers
* Customer segmentation (Gold, Silver, Bronze)
* Final report with all details
______________________________________________________________________________________________________________________________________________________

 **Challenges Faced: **
* Understanding how to build complete pipeline
* Writing correct join conditions
* Applying multiple transformations together
* Creating customer segmentation logic
______________________________________________________________________________________________________________________________________________________

 **Learnings:**
* How to build end-to-end data pipeline
* Importance of data cleaning
* How to generate business insights
* How SQL logic is similar to PySpark
* Better understanding of real-world data processing
______________________________________________________________________________________________________________________________________________________

 **Files in this Folder:**
* solution.py -> PySpark implementation
* phase4_problem_statement.pdf -> Problem description
* outputs/ -> Output results

