
### Objective

The objective of this assignment is to understand and implement advanced SQL concepts such as:

* Conditional logic using CASE WHEN
* Nested CASE statements
* Window functions like ROW_NUMBER(), RANK(), and DENSE_RANK()
____________________________________________________________________________________________________________________________________________________

### Problem Statement (Summary)

The assignment focuses on solving real-world business scenarios using SQL. It involves:

* Creating employee datasets
* Applying conditional logic for salary hikes, bonuses, and categorization
* Performing ranking and ordering using window functions
* Generating meaningful outputs based on business rules
____________________________________________________________________________________________________________________________________________________

### Dataset Used

### Employee Dataset (CASE WHEN)

Contains:

* Employee ID
* Employee Name
* Department
* Salary
* Experience
* Performance Rating

### Employees Dataset (Window Functions)

Contains:

* Employee ID
* Employee Name
* Department
* Salary
* Joining Date

### Orders Dataset

Contains:

* Order ID
* Customer Name
* City
* Order Amount
* Order Date
____________________________________________________________________________________________________________________________________________________


##  Approach

1. Created tables and inserted data using SQL
2. Applied CASE WHEN logic for business conditions
3. Used nested CASE statements for complex rules
4. Implemented window functions for ranking and ordering
5. Validated results using sample queries
____________________________________________________________________________________________________________________________________________________


##  Key Transformations

###  CASE WHEN

* Used for conditional logic
* Applied for salary hike, bonus calculation, and categorization

###  Nested CASE

* Used for multi-level decision-making
* Combined multiple conditions

###  Window Functions

* ROW_NUMBER() -> Assign unique sequence
* RANK() -> Ranking with gaps
* DENSE_RANK() -> Ranking without gaps
____________________________________________________________________________________________________________________________________________________

### Key Use Cases Implemented

### CASE WHEN Scenarios

* Salary hike based on experience and performance
* Bonus calculation based on department and performance
* Employee categorization (High, Mid, Low Performer)
* Risk assessment based on department and experience

### Nested CASE Scenarios

* Salary hike based on salary range + experience
* Bonus based on department + experience
* Tax bracket classification
* Promotion eligibility

### Window Function Scenarios

* Ranking employees by salary
* Ranking within departments
* Row numbering based on joining date
* Ranking orders by amount and city
____________________________________________________________________________________________________________________________________________________

### Output 

The outputs include:

* Categorized employees (High/Mid/Low performers)
* Salary hike and bonus calculations
* Ranked employee lists
* Ordered datasets using window functions
____________________________________________________________________________________________________________________________________________________

### Challenges Faced

* Writing complex CASE WHEN conditions
* Handling nested logic correctly
* Understanding differences between RANK and DENSE_RANK
* Applying partitioning in window functions
____________________________________________________________________________________________________________________________________________________

### Learnings

* Learned how to implement business logic using SQL
* Understood real-world use of CASE WHEN
* Gained knowledge of window functions and ranking
* Improved query writing and debugging skills
____________________________________________________________________________________________________________________________________________________

 ### Files in this Folder

* case_when.sql -> CASE WHEN solutions
* nested_case.sql -> Nested CASE problems
* window_functions.sql -> Window function queries
* dataset.sql -> Table creation and insert statements
* README.md -> Project documentation


