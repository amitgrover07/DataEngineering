-------------------------------------------------------------------------------------------------------------
-- 1. Retailer Database Tables (JSON)
-------------------------------------------------------------------------------------------------------------

-- Orders
CREATE EXTERNAL TABLE IF NOT EXISTS `dataengineering-project-492505.bronze_dataset.orders`(
    order_id INT64,
    customer_id INT64,
    order_date STRING,
    total_amount FLOAT64,
    updated_at STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://retailer-datalake-project-06042026/landing/retailer-db/orders/*.json'],
  ignore_unknown_values = true
);

-- Customers
CREATE EXTERNAL TABLE IF NOT EXISTS `dataengineering-project-492505.bronze_dataset.customers`
(
    customer_id INT64,
    name STRING,
    email STRING,
    updated_at STRING
)
OPTIONS (
    format = 'JSON',
    uris = ['gs://retailer-datalake-project-06042026/landing/retailer-db/customers/*.json'],
    ignore_unknown_values = true
);

-- Products
CREATE EXTERNAL TABLE IF NOT EXISTS `dataengineering-project-492505.bronze_dataset.products`
(
    product_id INT64,
    name STRING,
    category_id INT64,
    price FLOAT64,
    updated_at STRING
)
OPTIONS (
    format = 'JSON',
    uris = ['gs://retailer-datalake-project-06042026/landing/retailer-db/products/*.json'],
    ignore_unknown_values = true
);

-- Categories
CREATE EXTERNAL TABLE IF NOT EXISTS `dataengineering-project-492505.bronze_dataset.categories`
(
    category_id INT64,
    name STRING,
    updated_at STRING
)
OPTIONS (
    format = 'JSON',
    uris = ['gs://retailer-datalake-project-06042026/landing/retailer-db/categories/*.json'],
    ignore_unknown_values = true
);

-- Order Items
CREATE EXTERNAL TABLE IF NOT EXISTS `dataengineering-project-492505.bronze_dataset.order_items`
(
    order_item_id INT64,
    order_id INT64,
    product_id INT64,
    quantity INT64,
    price FLOAT64,
    updated_at STRING
)
OPTIONS (
    format = 'JSON',
    uris = ['gs://retailer-datalake-project-06042026/landing/retailer-db/order_items/*.json'],
    ignore_unknown_values = true
);

-------------------------------------------------------------------------------------------------------------
-- 2. Supplier Database Tables (JSON)
-------------------------------------------------------------------------------------------------------------

-- Suppliers
CREATE EXTERNAL TABLE IF NOT EXISTS `dataengineering-project-492505.bronze_dataset.suppliers` (
    supplier_id INT64,
    supplier_name STRING,
    contact_name STRING,
    phone STRING,
    email STRING,
    address STRING,
    city STRING,
    country STRING,
    created_at STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://retailer-datalake-project-06042026/landing/supplier-db/suppliers/*.json'],
  ignore_unknown_values = true
);

-- Product Suppliers
CREATE EXTERNAL TABLE IF NOT EXISTS `dataengineering-project-492505.bronze_dataset.product_suppliers` (
    supplier_id INT64,
    product_id INT64,
    supply_price FLOAT64,
    last_updated STRING
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://retailer-datalake-project-06042026/landing/supplier-db/product_suppliers/*.json'],
  ignore_unknown_values = true
);

-------------------------------------------------------------------------------------------------------------
-- 3. Customer Reviews (Parquet)
-------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE EXTERNAL TABLE `dataengineering-project-492505.bronze_dataset.customer_reviews` (
  id STRING,
  customer_id INT64,
  product_id INT64,
  rating INT64,
  review_text STRING,
  review_date STRING
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://retailer-datalake-project-06042026/landing/customer_reviews/customer_reviews_*.parquet']
);