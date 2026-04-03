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


-- 1. Total order amount for each customer
SELECT c.first_name, SUM(s.total_amount) AS total_amount
FROM customers c
JOIN sales s
ON c.customer_id = s.customer_id
GROUP BY c.first_name;

-- 2. Top 3 customers by total spend
SELECT c.first_name, SUM(s.total_amount) AS total_spend
FROM customers c
JOIN sales s
ON c.customer_id = s.customer_id
GROUP BY c.first_name
ORDER BY total_spend DESC
LIMIT 3;

-- 3. Customers with no orders
SELECT c.first_name
FROM customers c
LEFT JOIN sales s
ON c.customer_id = s.customer_id
WHERE s.customer_id IS NULL;

-- 4. City-wise total revenue
SELECT c.city, SUM(s.total_amount) AS total_revenue
FROM customers c
JOIN sales s
ON c.customer_id = s.customer_id
GROUP BY c.city;

-- 5. Average order amount per customer
SELECT customer_id, AVG(total_amount) AS avg_amount
FROM sales
GROUP BY customer_id;

-- 6. Customers with more than one order
SELECT customer_id, COUNT(*) AS order_count
FROM sales
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 7. Sort customers by total spend descending
SELECT c.first_name, SUM(s.total_amount) AS total_spend
FROM customers c
JOIN sales s
ON c.customer_id = s.customer_id
GROUP BY c.first_name
ORDER BY total_spend DESC;
