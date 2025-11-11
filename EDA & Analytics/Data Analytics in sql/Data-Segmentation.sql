-- Products cost segmentation 
---------------------------------------------
use DataWarehouse
go
WITH product_segments AS (
SELECT 
    product_key,
    product_name,
    cost,
    CASE 
        WHEN cost < 100 THEN 'Below 100'
        WHEN cost BETWEEN 100 AND 500 THEN '100-500'
        WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
        ELSE 'Above 1000'
    END AS cost_range
FROM gold.dim_product
)
SELECT 
    cost_range,
    COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC
--------------------------------------------------------
-- Customers Segmentation 
---------------------------------------------
use DataWarehouse
go
WITH customer_spending AS (
SELECT 
customerr.customer_key,
sum(fact.sales_amount) AS Total_spending,
min(fact.order_date) AS first_order_date,
max(fact.order_date) AS last_order_date,
DATEDIFF(MONTH,min(fact.order_date),max(fact.order_date)) AS life_span
from
gold.fact_sales fact LEFT JOIN gold.dim_customers customerr
on 
fact.customer_key = customerr.customer_key
group by
customerr.customer_key 
)

SELECT
customer_segment,
COUNT(customer_key) AS total_customers
from 

(
SELECT
customer_key,
Total_spending,
life_span,
CASE WHEN life_span >= 12 AND Total_spending > 5000 THEN 'VIP'
     WHEN life_span >= 12 AND Total_spending <= 5000 THEN 'Regular' 
     else 'New'
END AS customer_segment
from
customer_spending ) t
group by
customer_segment
order by total_customers desc
