CREATE TABLE customers (
 customer_id INT,
 customer_name VARCHAR(50),
 city VARCHAR(50),
 age INT
);
INSERT INTO customers VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, 'Sita', 'Chennai', 32),
(3, 'Arun', 'Hyderabad', 28);


 # Implementing the codes
- Show all the customers**
    SELECT * 
     FROM customers;
  
- Show customers  from Chennai
  customers.filter("city = 'Chennai'").show()

- Show customers with age> 25
  customers.filter(customers.age > 25).show()

- Show inly customer_name and city
   customers.select("customer_name", "city").show()

- count customers city-wise
   customers.groupBy("city").count().show()
