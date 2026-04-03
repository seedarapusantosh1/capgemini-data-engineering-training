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
    SELECT * FROM customers;
  
- Show customers  from Chennai
     SELECT * FROM customers WHERE city = 'Chennai';

- Show customers with age> 25
  SELECT * FROM customers WHERE age > 25;

- Show inly customer_name and city
   SELECT customer_name, city FROM customers;

- count customers city-wise
  SELECT city, COUNT(*) AS total_customers FROM customers
  GROUP BY city;
