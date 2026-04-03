-- Create Customers Table
CREATE TABLE customers (
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone_number VARCHAR(20),
    address VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(10),
    zip_code VARCHAR(10)
);

-- Insert Customers Data
INSERT INTO customers VALUES
(1, 'John', 'Smith', 'john.smith@domain.com', '555-0001', '123 Elm St', 'Springfield', 'IL', '62701'),
(2, 'Emma', 'Jones', 'emma.jones@webmail.com', '555-0002', '456 Oak St', 'Centerville', 'OH', '45459'),
(3, 'Olivia', 'Brown', 'olivia.brown@outlook.com', '555-0003', '789 Pine St', 'Greenville', 'SC', '29601'),
(4, 'Liam', 'Johnson', 'liam.johnson@gmail.com', '555-0004', '101 Maple St', 'Riverside', 'CA', '92501'),
(5, 'Noah', 'Williams', 'noah.williams@yahoo.com', '555-0005', '202 Birch St', 'Lakeside', 'TX', '75001');

-- Create Sales Table
CREATE TABLE sales (
    sale_id INT,
    customer_id INT,
    product_id INT,
    sale_date DATE,
    quantity INT,
    total_amount FLOAT
);

-- Insert Sales Data
INSERT INTO sales VALUES
(1, 1, 1, '2024-01-15', 2, 39.98),
(2, 1, 3, '2024-01-20', 1, 29.99),
(3, 2, 2, '2024-01-16', 1, 25.00),
(4, 2, 4, '2024-01-22', 3, 89.97),
(5, 3, 5, '2024-01-17', 2, 49.98);



-- 1. Daily sales

SELECT sale_date, SUM(total_amount) AS daily_sales
FROM sales
GROUP BY sale_date;

-- 2. City-wise revenue

SELECT c.city, SUM(s.total_amount) AS total_revenue
FROM customers c
JOIN sales s
ON c.customer_id = s.customer_id
GROUP BY c.city;

-- 3. Repeat customers (>2 orders)

  SELECT CONCAT(c.first_name," ",last_name) as cus_name,COUNT(s.sale_id) AS total_orders
  FROM customers c
  JOIN sales s
  ON c.customer_id=s.customer_id
  GROUP BY cus_name
  HAVING total_orders>1

-- 4. Highest spending customer in each city

SELECT c.city, c.first_name, SUM(s.total_amount) AS total_spend
FROM customers c
JOIN sales s
ON c.customer_id = s.customer_id
GROUP BY c.city, c.first_name
ORDER BY c.city, total_spend DESC;

-- 5. Final reporting table

SELECT c.customer_id, c.first_name, c.city,
       SUM(s.total_amount) AS total_spend,
       COUNT(s.sale_id) AS order_count
FROM customers c
JOIN sales s
ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.first_name, c.city;

