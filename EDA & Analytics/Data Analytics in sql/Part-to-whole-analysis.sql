-- Which category contributes the most to overall sales
-------------------------------------------------------
use DataWarehouse
go
with category_sales AS (
SELECT 
productt.category,
SUM(sales_amount) AS Total_sales_for_each_category
from
gold.fact_sales fact LEFT JOIN  gold.dim_product productt
on 
fact.product_key = productt.product_key
group by
category
)
select
category,
Total_sales_for_each_category,
sum(Total_sales_for_each_category) OVER () AS Sales_for_all_categories,
CONCAT(ROUND (CAST (Total_sales_for_each_category AS FLOAT) / sum(Total_sales_for_each_category) OVER () * 100,2) , '%') AS Per_of_total_sales
from
category_sales
----------------------------------------------------------
-- Which subcategory contributes the most to overall sales
----------------------------------------------------------
use DataWarehouse
go
with subcategory_sales AS (
SELECT 
productt.subcategory,
SUM(sales_amount) AS Total_sales_for_each_subcategory
from
gold.fact_sales fact LEFT JOIN  gold.dim_product productt
on 
fact.product_key = productt.product_key
group by
subcategory
)
select
subcategory,
Total_sales_for_each_subcategory,
sum(Total_sales_for_each_subcategory) OVER () AS Total_Sales_for_all_subcategories,
CONCAT(ROUND (CAST (Total_sales_for_each_subcategory AS FLOAT) / sum(Total_sales_for_each_subcategory) OVER () * 100,2) , '%') AS Per_of_total_sales
from
subcategory_sales
