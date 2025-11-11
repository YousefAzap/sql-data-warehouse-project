use DataWarehouse
go
-- Creating the view containing the hole result
CREATE VIEW gold.report_customers AS
-- First general query retreving all the nedded data without any aggregations or calculations
WITH base_query AS (
SELECT
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
CONCAT (C.first_name ,' ',c.last_name) AS Customer_name ,
DATEDIFF(YEAR,c.birthdate,GETDATE()) AS Age,
c.customer_number
from
gold.fact_sales f LEFT JOIN gold.dim_customers c
on f.customer_key = c.customer_key
where
order_date IS NOT NULL 
),
-- Second CTE (subset of the base_query) doing all the needed aggregations (customer_aggregation)
customer_aggregation AS (
SELECT
customer_key,
customer_number,
Customer_name,
Age,
sum (sales_amount) AS Total_sales,
sum (quantity) AS Total_quatity,
count (DISTINCT order_number) AS Number_of_orders ,
Count (DISTINCT product_key)  AS Number_of_produsts ,
max(order_date)  AS Last_order_date ,
DATEDIFF(MONTH,MIN(order_date),max(order_date)) AS Lifespan
FROM base_query
GROUP BY
customer_key,
customer_number,
Customer_name,
Age
)
SELECT
customer_key,
customer_number,
Customer_name,
Age,
CASE
    WHEN age < 20 THEN 'Under 20'
    WHEN age BETWEEN 20 AND 29 THEN '20-29'
    WHEN age BETWEEN 30 AND 39 THEN '30-39'
    WHEN age BETWEEN 40 AND 49 THEN '40-49'
    ELSE '50 and above'
END AS age_group,
Total_sales,
Total_quatity,
Number_of_orders ,
Number_of_produsts ,
Last_order_date ,
-- Calculating Recency for each customer
DATEDIFF(MONTH,Last_order_date,GETDATE()) ASVRecency ,
Lifespan,
-- Calculating average order value (AOV)
CASE WHEN total_sales = 0 THEN 0
     ELSE total_sales / Number_of_orders
END AS avg_order_value,

-- Calculating average monthly spend
CASE WHEN lifespan = 0 THEN total_sales
     ELSE total_sales / lifespan
END AS avg_monthly_spend,

CASE WHEN Lifespan >= 12 AND Total_sales > 5000 THEN 'VIP'
     WHEN Lifespan >= 12 AND Total_sales <= 5000 THEN 'Regular' 
     else 'New'
END AS customer_segment
FROM
customer_aggregation
