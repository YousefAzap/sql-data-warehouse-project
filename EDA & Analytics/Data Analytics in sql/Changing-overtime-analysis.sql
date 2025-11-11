-- Changing overtime analysis
select 
YEAR (order_date) AS order_year,
MONTH (order_date) AS order_month,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT customer_key) AS total_customers
from
gold.fact_sales
where order_date IS NOT NULL
group by 
YEAR (order_date),
MONTH (order_date)
order by
YEAR (order_date),
MONTH (order_date)
------------------------------------------------
-- Changing overtime analysis with datetrunc (the output is a date)
select 
DATETRUNC(MONTH,order_date) AS order_date,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT customer_key) AS total_customers
from
gold.fact_sales
where order_date IS NOT NULL
group by 
DATETRUNC(MONTH,order_date)
order by
DATETRUNC(MONTH,order_date)
------------------------------------------------
-- Changing overtime analysis with customized format (the output is a not a date (string)), (yyyy) not capital 
select 
FORMAT(order_date,'yyyy-MMM') AS order_date,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT customer_key) AS total_customers
from
gold.fact_sales
where order_date IS NOT NULL
group by 
FORMAT(order_date,'yyyy-MMM')
order by
FORMAT(order_date,'yyyy-MMM')
