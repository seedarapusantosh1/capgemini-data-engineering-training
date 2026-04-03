 **Objective:**
 
  In this phase, I learned how to build a complete ETL (Extract, Transform, Load) pipeline using PySpark. The focus was on reading data, cleaning      it, transforming it, and generating useful outputs.
_____________________________________________________________________________________________________________________________________________________

 **Problem Summary:**

  In this phase, I worked on real-world data engineering tasks such as:

- Reading data from CSV, JSON, and Parquet files.
- Handling missing and invalid data.
- Applying filters and transformations.
- Performing aggregations and joins.
- Building a complete data pipeline.
_____________________________________________________________________________________________________________________________________________________

**Approach:**
- First i extracted data from files using PySpark
- Aftr inspected data using **show()** and **printSchema()**
- Then cleaned data by removing null values using **dropna()**
- Later filtered invalid records
- **Transformed data using:**

  - groupBy and aggregations
  - joins between datasets
_____________________________________________________________________________________________________________________________________________________

**Key Transformations Used:**

- **read()** -> to load data
- **dropna() / fillna()** -> to handle missing values
- **filter()** -> to remove invalid data
- **join()** -> to combine datasets
- **groupBy()**-> to group data
- **agg()** -> to calculate metrics
- **write()**-> to store output
____________________________________________________________________________________________________________________________________________________

 **Outputs:**

- Daily sales calculation
- City-wise revenue
- Repeat customers identification
- Highest spending customers
- Final reporting dataset with:

  * customer details
  * city
  * total spend
  * order count  _____________________________________________________________________________________________________________________________________________________

**Challenges Faced**

- Understanding ETL pipeline structure
- Handling missing and incorrect data
- Writing logic for multiple transformations
- Combining all steps into one workflow
_____________________________________________________________________________________________________________________________________________________

**Learnings:**
    From this phase3 i learns the following things:
* Through this i Understanding ETL process (Extract, Transform, Load).
* Reading different file formats in PySpark.
* Data cleaning techniques.
* Building data pipelines step-by-step.
* Converting SQL logic into PySpark.
_____________________________________________________________________________________________________________________________________________________

**Files in this Folder:**

* solution.py -> PySpark ETL pipeline code
* phase3_problem_statement.pdf -> Problem description
* outputs/ -> Output screenshots


