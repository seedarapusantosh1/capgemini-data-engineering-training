
##  Objective

The objective of this assignment is to analyze student submission data using SQL by applying joins, window functions, and data transformation techniques. The goal is to identify inconsistencies, duplicates, and participation patterns.
_____________________________________________________________________________________________________________________________________________________

##  Problem Statement (Summary)

We are given three datasets related to student submissions:

* Master table (student details)
* Task1 responses (email-based submissions)
* Task1 file2 (contains duplicates and invalid records)

The task is to:

* Clean and normalize data
* Map emails to student IDs
* Identify valid, invalid, and missing submissions
* Detect duplicate submissions
* Generate meaningful insights
_____________________________________________________________________________________________________________________________________________________

##  Dataset Description

### 1️ Master Table

* Contains 56 unique students
* Fields:

  * student_id
  * college_email
  * personal_email

### 2️ Task1_Responses

* Contains 51 records
* Based on student email submissions

### 3️ Task1_File2

* Contains 60 records
* Includes:

  * Duplicate entries
  * Invalid emails
  * Extra submissions
_____________________________________________________________________________________________________________________________________________________


##  Approach

###  Phase 1: Data Preparation

* Converted emails to lowercase using **LOWER()**
* Removed spaces using **TRIM()**
* Created a unified email mapping table
* Mapped both college and personal emails to a single **student_id**


###  Phase 2: Core Analysis

* **Not Submitted Students**

```sql
SELECT m.student_id
FROM master m
LEFT JOIN responses r
ON m.email = r.email
WHERE r.email IS NULL;
```

* **Valid Submissions**

```sql
SELECT *
FROM responses r
INNER JOIN master m
ON r.email = m.email;
```

* **Invalid Submissions**

```sql
SELECT *
FROM responses r
LEFT JOIN master m
ON r.email = m.email
WHERE m.student_id IS NULL;
```

---

###  Phase 3: Duplicate Detection

Used window function:

```sql
ROW_NUMBER() OVER (
PARTITION BY student_id
ORDER BY timestamp
)
```

* First record → valid
* Remaining → duplicates

---

###  Phase 4: Advanced Insights

* Count submissions per student
* Identify students using multiple emails
* Classify students as:

  * Submitted
  * Not Submitted
  * Duplicate
  * Invalid
_____________________________________________________________________________________________________________________________________________________

##  Key SQL Concepts Used

* Joins (INNER JOIN, LEFT JOIN)
* Window Functions (ROW_NUMBER)
* GROUP BY
* COALESCE
* String functions (LOWER, TRIM)
_____________________________________________________________________________________________________________________________________________________

##  Output

* Identified students who did not submit
* Detected duplicate submissions
* Found invalid email records
* Generated clean and validated dataset
_____________________________________________________________________________________________________________________________________________________

##  Challenges Faced

* Handling multiple email sources
* Identifying duplicates correctly
* Ensuring accurate joins
* Managing inconsistent data
_____________________________________________________________________________________________________________________________________________________

##  Learnings

* Importance of data cleaning before analysis
* Difference between GROUP BY and window functions
* How to detect duplicates using ROW_NUMBER
* Real-world data validation techniques
_____________________________________________________________________________________________________________________________________________________


##  Files in this Folder

* master_table.sql -> Student master data
* responses.sql -> Task1 responses
* file2.sql -> Additional dataset with duplicates
* queries.sql -> SQL solutions
* README.md -> Project documentation


