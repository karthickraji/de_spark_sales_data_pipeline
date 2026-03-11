#Total Revenue
SELECT SUM(revenue)
FROM sales_processed;

#Top Products
SELECT product, SUM(revenue) as total_revenue
FROM sales_processed
GROUP BY product
ORDER BY total_revenue DESC
LIMIT 10;

#Revenue by Country
SELECT country, SUM(revenue)
FROM sales_processed
GROUP BY country;