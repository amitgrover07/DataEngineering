-----------------------------------------------------------------------------------------------------------
-- 1. Sales Summary (sales_summary)
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `dataengineering-project-492505.gold_dataset.sales_summary`
PARTITION BY order_date
AS
SELECT 
    DATE(CAST(o.order_date AS TIMESTAMP)) AS order_date,
    p.category_id,
    c.name AS category_name,
    oi.product_id,
    p.name AS product_name,
    SUM(oi.quantity) AS total_units_sold,
    ROUND(SUM(oi.price * oi.quantity), 2) AS total_sales,
    COUNT(DISTINCT o.customer_id) AS unique_customers
FROM `dataengineering-project-492505.silver_dataset.orders` o
JOIN `dataengineering-project-492505.silver_dataset.order_items` oi ON o.order_id = oi.order_id
JOIN `dataengineering-project-492505.silver_dataset.products` p ON oi.product_id = p.product_id
JOIN `dataengineering-project-492505.silver_dataset.categories` c ON p.category_id = c.category_id
WHERE o.is_active = TRUE
GROUP BY 1, 2, 3, 4, 5;

-----------------------------------------------------------------------------------------------------------
-- 2. Customer Engagement Metrics (customer_engagement)
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `dataengineering-project-492505.gold_dataset.customer_engagement`
AS
SELECT 
    c.customer_id,
    c.name AS customer_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(oi.price * oi.quantity), 2) AS total_spent,
    MAX(CAST(o.order_date AS TIMESTAMP)) AS last_order_date,
    DATE_DIFF(CURRENT_DATE(), DATE(MAX(CAST(o.order_date AS TIMESTAMP))), DAY) AS days_since_last_order,
    SAFE_DIVIDE(SUM(oi.price * oi.quantity), COUNT(DISTINCT o.order_id)) AS avg_order_value
FROM `dataengineering-project-492505.silver_dataset.customers` c
LEFT JOIN `dataengineering-project-492505.silver_dataset.orders` o ON c.customer_id = o.customer_id
LEFT JOIN `dataengineering-project-492505.silver_dataset.order_items` oi ON o.order_id = oi.order_id
WHERE c.is_active = TRUE
GROUP BY 1, 2;

-----------------------------------------------------------------------------------------------------------
-- 3. Product Performance (product_performance)
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `dataengineering-project-492505.gold_dataset.product_performance`
AS
WITH sales_agg AS (
    SELECT 
        product_id,
        SUM(quantity) AS total_units_sold,
        SUM(price * quantity) AS total_revenue
    FROM `dataengineering-project-492505.silver_dataset.order_items`
    GROUP BY 1
),
reviews_agg AS (
    SELECT 
        product_id,
        AVG(rating) AS avg_rating,
        COUNT(review_text) AS total_reviews
    FROM `dataengineering-project-492505.silver_dataset.customer_reviews`
    GROUP BY 1
)
SELECT 
    p.product_id,
    p.name AS product_name,
    p.category_id,
    c.name AS category_name,
    ps.supplier_id,
    s.supplier_name,
    COALESCE(sa.total_units_sold, 0) AS total_units_sold,
    ROUND(COALESCE(sa.total_revenue, 0), 2) AS total_revenue,
    ROUND(COALESCE(ra.avg_rating, 0), 1) AS avg_rating,
    COALESCE(ra.total_reviews, 0) AS total_reviews
FROM `dataengineering-project-492505.silver_dataset.products` p
LEFT JOIN `dataengineering-project-492505.silver_dataset.categories` c ON p.category_id = c.category_id
LEFT JOIN `dataengineering-project-492505.silver_dataset.product_suppliers` ps ON p.product_id = ps.product_id
LEFT JOIN `dataengineering-project-492505.silver_dataset.suppliers` s ON ps.supplier_id = s.supplier_id
LEFT JOIN sales_agg sa ON p.product_id = sa.product_id
LEFT JOIN reviews_agg ra ON p.product_id = ra.product_id
WHERE p.is_quarantined = FALSE;

-----------------------------------------------------------------------------------------------------------
-- 4. Supplier Performance (supplier_analysis)
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `dataengineering-project-492505.gold_dataset.supplier_analysis`
AS
WITH supp_sales AS (
    SELECT 
        ps.supplier_id,
        COUNT(DISTINCT ps.product_id) AS total_products_supplied,
        SUM(oi.quantity) AS total_units_sold,
        SUM(oi.price * oi.quantity) AS total_revenue
    FROM `dataengineering-project-492505.silver_dataset.product_suppliers` ps
    LEFT JOIN `dataengineering-project-492505.silver_dataset.order_items` oi ON ps.product_id = oi.product_id
    GROUP BY 1
)
SELECT 
    s.supplier_id,
    s.supplier_name,
    COALESCE(ss.total_products_supplied, 0) AS total_products_supplied,
    COALESCE(ss.total_units_sold, 0) AS total_units_sold,
    ROUND(COALESCE(ss.total_revenue, 0), 2) AS total_revenue
FROM `dataengineering-project-492505.silver_dataset.suppliers` s
LEFT JOIN supp_sales ss ON s.supplier_id = ss.supplier_id
WHERE s.is_quarantined = FALSE;

-----------------------------------------------------------------------------------------------------------
-- 5. Customer Reviews Summary (customer_reviews_summary)
-----------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `dataengineering-project-492505.gold_dataset.customer_reviews_summary`
AS
SELECT 
    p.product_id,
    p.name AS product_name,
    ROUND(AVG(cr.rating), 1) AS avg_rating,
    COUNT(cr.review_text) AS total_reviews,
    COUNT(IF(cr.rating >= 4, 1, NULL)) AS positive_reviews,
    COUNT(IF(cr.rating <= 2, 1, NULL)) AS negative_reviews
FROM `dataengineering-project-492505.silver_dataset.products` p
LEFT JOIN `dataengineering-project-492505.silver_dataset.customer_reviews` cr ON p.product_id = cr.product_id
WHERE p.is_quarantined = FALSE
GROUP BY 1, 2;