**Objective**
  The goal of this phase is to work with a real-world dataset and build a complete data engineering pipeline using Databricks and PySpark. This phase focuses on data ingestion, validation, transformation, and advanced analytics using window functions.
_________________________________________________________________________________________________________________________________________________
**Problem Statement (Summary)**
   - Set up Databricks environment and upload dataset
   - Work with multiple tables from a real-world e-commerce dataset
   - Perform data validation and ensure referential integrity
   - Calculate top customers per city using window functions
   - Compute running total of daily sales
   - Identify top products per category using ranking
   - Calculate customer lifetime value (CLV)
   - Perform customer segmentation (Gold, Silver, Bronze)
   - Build a final reporting dataset combining all insights
_________________________________________________________________________________________________________________________________________________
**Dataset Used**
  Datasets: Olist Brazilian E-commerce Dataset
  Source: kaggle
  Tables: orders, customers, order_items, products
__________________________________________________________________________________________________________________________________________________
**Approach**
1. Loaded the datasets into PySpark DataFrames
2.Cleaned the data by removing invalid or null records
3.Validated relationships between datasets
4.Applied joins to combine data
5.Performed aggregations and analysis
6.Built a complete pipeline from start to end
____________________________________________________________________________________________________________________________________________________
**Key Transformations**

- Used joins (inner, left, anti) to combine datasets
- Applied window functions like rank, row_number, and running totals
- Performed aggregations using groupBy
- Used date functions for monthly analysis
- Handled null values and data cleaning
____________________________________________________________________________________________________________________________________________________
**Output**

- Generated final dataset after applying all transformations
- Produced insights like:
- Top customers
- Monthly sales trends
- Running totals
- Ensured the output is clean, validated, and ready for further analysis
____________________________________________________________________________________________________________________________________________________
**Challenges Faced**

1. Initially, understanding window functions was difficult, especially concepts like ranking and lag
2.Faced issues while performing joins, particularly handling null values and unmatched records
3.Debugging PySpark errors took time because error messages were not always clear
4.Managing multiple transformations in a single pipeline was slightly confusing at first
5.Ensuring data correctness after each step required careful validation
_____________________________________________________________________________________________________________________________________________________
**Learnings**

- Gained strong understanding of joins and how datasets are connected
- Learned how to use window functions for advanced data analysis
- Improved skills in data cleaning and handling null values
- Understood how to build a complete data pipeline step by step
- Got better at debugging and breaking down problems into smaller parts
- Learned the importance of validating data at each stage
____________________________________________________________________________________________________________________________________________________
**Files in the Folder**
  - phase5_problem_statement.pdf -> Problem description
  - outputs.py -> ImplementationE
  - outputs/ -> Final results
