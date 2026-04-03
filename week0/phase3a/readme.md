**Objective:**
Work with messy data and clean it before using it. This phase helps to understand why data cleaning is important in real-world data engineering.

___________________________________________________________________________________________________________________________________________________

**Problem Statement:**

- Identify issues like null values, duplicates, and wrong data
- Remove rows where important fields like customer_id are missing
- Handle missing values in columns like name and city
- Remove duplicate records
- Remove invalid data (for example, negative age)
- Check data before and after cleaning
- Count number of customers in each city
_____________________________________________________________________________________________________________________________________________________

**Dataset Used:**

- Dataset: customers
- Source: given dataset
- Table: customers
_____________________________________________________________________________________________________________________________________________________

**Approach:**

1. First, I checked the dataset to understand the data
2. Found null values, duplicate records, and invalid data
3. Cleaned the data using dropna() and fillna()
4. Removed duplicates and filtered wrong values
5. Finally, grouped data to count customers per city
______________________________________________________________________________________________________________________________________________________

**Key Transformations:**

1. dropna() – used to remove rows with missing important values
2. fillna() – used to fill missing values
3. filter() – used to remove invalid data (like negative age)
4. groupBy() and count() – used to get customer count per city
______________________________________________________________________________________________________________________________________________________

**Output:**

* Clean dataset with correct values
* Removed nulls, duplicates, and invalid data
* Verified data after cleaning
* Got number of customers in each city
_____________________________________________________________________________________________________________________________________________________

**Challenges Faced:**

1. Finding all issues in the dataset
2. Deciding which data to remove or keep
3. Handling duplicates properly
4. Making sure correct data is not removed
5. Checking if data is cleaned correctly
______________________________________________________________________________________________________________________________________________________

**Learnings:**

* Real data is not always clean
* Cleaning is very important before analysis
* Small mistakes in data can give wrong results
* Learned how to handle nulls, duplicates, and invalid data step by step
______________________________________________________________________________________________________________________________________________________

**Files in this Folder:**

* phase3a_data_quality_challenge.pdf -> Problem description
* pyspark_code.py -> PySpark implementation
* outputs/ -> Output results

