
 **Objective**

The objective of this phase is to perform data transformations on structured datasets using PySpark and SQL. This includes joining multiple tables, applying aggregations, and generating meaningful business insights.
____________________________________________________________________________________________________________________________________________________

**Problem Statement (Summary)**

In this task, we were given multiple datasets such as:

* Employee table (employee details)
* Sales table (transaction data)

The goal was to:

* Combine data from multiple tables
* Perform aggregations using GROUP BY
* Apply filtering and conditions
* Generate insights such as total sales, department performance, and employee metrics
____________________________________________________________________________________________________________________________________________________

 **Dataset Used**

### Employee Table

Contains:

* Employee ID, Name
* Department
* Salary
* Joining Date

### Sales Table

Contains:

* Sales ID
* Employee ID
* Product
* Sales Amount
* Sale Date
____________________________________________________________________________________________________________________________________________________

 **Approach**

1. Created tables and inserted data using SQL
2. Loaded datasets into PySpark DataFrames
3. Performed data cleaning:

   * Handled NULL values
   * Removed invalid data (if any)
4. Joined Employee and Sales tables using `emp_id`
5. Applied transformations:

   * GROUP BY aggregations
   * Filtering using conditions
6. Generated final insights
____________________________________________________________________________________________________________________________________________________

 **Key Transformations Used**

* **join()**-> Combine Employee and Sales tables
* **groupBy()** -> Group data by department, employee, or product
* **agg()**-> Calculate metrics like sum, count, avg
* **filter()** -> Apply conditions on data
* **orderBy()** -> Sort results
____________________________________________________________________________________________________________________________________________________

 **Output**

The following outputs were generated:

* Department-wise salary summary
* Employee-wise sales performance
* Product-wise sales insights
* Top-performing employees

____________________________________________________________________________________________________________________________________________________

**Challenges Faced**

* Understanding join conditions between Employee and Sales tables
* Handling aggregation logic correctly
* Writing efficient GROUP BY queries
* Debugging SQL and PySpark errors
____________________________________________________________________________________________________________________________________________________

 **Learnings**

* Strong understanding of GROUP BY and aggregations
* Learned how joins work in real datasets
* Improved data cleaning and validation techniques
* Understood how to generate business insights from data
____________________________________________________________________________________________________________________________________________________

**Files in this Folder**

* employee_sales.sql-> Table creation and insert queries
* queries.sql -> All 30 GROUP BY queries
* pyspark_solution.py -> PySpark implementation
* outputs/ -> Output results and screenshots
* README.md -> Project documentation


