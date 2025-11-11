-- Calculating the running total of the sales overtime
select
order_date,
total_sales,
SUM(total_sales) over (order by order_date) AS Running_Total,
AVG(Average_price) over (order by order_date) AS Moving_Averages
from
(
select 
DATETRUNC(MONTH,order_date) AS order_date,
SUM(sales_amount) AS total_sales,
AVG(price) AS Average_price
from
gold.fact_sales
where order_date IS NOT NULL
group by 
DATETRUNC(MONTH,order_date)
)t
