--Analyze the yearly performance of products by comparing their sales to both the average sales performance of the product and the previous year's sales 
---------------------------------------------------------------------------------------------------------------------------------------------------------
WITH yearly_product_sales AS (
select
YEAR(order_date) AS order_year,
productt.product_name,
SUM(sales_amount) AS total_sales_amount
from
gold.fact_sales f left join gold.dim_product productt
on f.product_key = productt.product_key
where
order_date IS NOT NULL
group by
YEAR(order_date),
productt.product_name
)
select
order_year,
product_name,
total_sales_amount,
AVG(total_sales_amount) OVER(PARTITION BY product_name ) AS Average_sales ,
total_sales_amount - AVG(total_sales_amount) OVER(PARTITION BY product_name ) AS diff_avg,
CASE WHEN total_sales_amount - AVG(total_sales_amount) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
     WHEN total_sales_amount - AVG(total_sales_amount) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
     ELSE 'Avg'
END avg_change,
LAG(total_sales_amount) OVER(PARTITION BY product_name ORDER BY order_year ) AS Last_year_sales,
total_sales_amount - LAG(total_sales_amount) OVER(PARTITION BY product_name ORDER BY order_year ) AS diff_Last_year,
CASE WHEN total_sales_amount - LAG(total_sales_amount) OVER(PARTITION BY product_name ORDER BY order_year ) > 0 THEN 'Increase'
     WHEN total_sales_amount - LAG(total_sales_amount) OVER(PARTITION BY product_name ORDER BY order_year ) < 0 THEN 'Decrease'
     ELSE 'No Change'
END Last_year_change
from
yearly_product_sales
order by
product_name,
order_year
