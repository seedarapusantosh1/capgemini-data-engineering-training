-- Create customers table
CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(50),
    city VARCHAR(50),
    age INT
);

-- Insert sample data into customers
INSERT INTO customers (customer_id, customer_name, city, age) VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, 'Sita', 'Chennai', 32),
(3, 'Arun', 'Hyderabad', 28),
(4, 'Meena', 'Bengaluru', 35),
(5, 'Kiran', 'Chennai', 22);

-- Create orders table
CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    amount INT,
    order_date DATE
);

-- Insert sample data into orders
INSERT INTO orders (order_id, customer_id, amount, order_date) VALUES
(101, 1, 2500, '2026-03-01'),
(102, 2, 1800, '2026-03-02'),
(103, 1, 3200, '2026-03-03'),
(104, 3, 1500, '2026-03-04'),
(105, 5, 2800, '2026-03-05');


Queries:

 1- Total order amount for each customer
  SELECT c.customer_name, SUM(o.amount) AS total_amount
  FROM customers c
  JOIN orders o 
  ON c.customer_id=o.customer_id
  GROUP BY c.customer_name
  
2- Top 3 customers by total spend
  SELECT c.customer_name, SUM(o.amount) AS total_amount
  FROM customers c
  JOIN orders o 
  ON c.customer_id=o.customer_id
  GROUP BY c.customer_name
  ORDER BY total_amount DESC LIMIT 3

3- Customers with no orders
  SELECT *
  FROM customers c
  WHERE NOT EXISTS ( SELECT 1 FROM orders o WHERE o.customer_id=c.customer_id)

4- City-wise total revenue
  SELECT c.city, SUM(o.amount) AS total_amount
  FROM customers c
  JOIN orders o 
  ON c.customer_id=o.customer_id
  GROUP BY c.city

 5- Average order amount per customer
  SELECT c.customer_name, AVG(o.amount) AS total_amount
  FROM customers c
  JOIN orders o 
  ON c.customer_id=o.customer_id
  GROUP BY c.customer_name

6- Customers with more than one order
  SELECT c.customer_name, COUNT(o.order_id) AS m_orders
  FROM customers c
  JOIN orders o 
  ON c.customer_id=o.customer_id
  GROUP BY c.customer_name
  HAVING m_orders>1

7- Sort customers by total spend descending
  SELECT c.customer_name, SUM(o.amount) AS total_amount
  FROM customers c
  JOIN orders o 
  ON c.customer_id=o.customer_id
  GROUP BY c.customer_name
  ORDER BY total_amount DESC 
