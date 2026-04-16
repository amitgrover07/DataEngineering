BEGIN
  -----------------------------------------------------------------------------------------------------------
  -- 1. CUSTOMERS (SCD Type 2)
  -----------------------------------------------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS `dataengineering-project-492505.silver_dataset.customers`
  (
      customer_id INT64, name STRING, email STRING, updated_at TIMESTAMP,
      is_quarantined BOOL, effective_start_date TIMESTAMP, effective_end_date TIMESTAMP, is_active BOOL
  ) CLUSTER BY customer_id;

  -- Inactivate records where data has changed
  MERGE `dataengineering-project-492505.silver_dataset.customers` T
  USING `dataengineering-project-492505.bronze_dataset.customers` S
  ON T.customer_id = S.customer_id AND T.is_active = TRUE
  WHEN MATCHED AND (T.name != S.name OR T.email != S.email) THEN
    UPDATE SET T.is_active = FALSE, T.effective_end_date = CURRENT_TIMESTAMP();

  -- Insert new or updated versions
  INSERT INTO `dataengineering-project-492505.silver_dataset.customers`
  SELECT 
    customer_id, name, email, SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%z', updated_at),
    (customer_id IS NULL OR email IS NULL) AS is_quarantined,
    CURRENT_TIMESTAMP(), NULL, TRUE
  FROM `dataengineering-project-492505.bronze_dataset.customers` S
  WHERE NOT EXISTS (SELECT 1 FROM `dataengineering-project-492505.silver_dataset.customers` T WHERE T.customer_id = S.customer_id AND T.is_active = TRUE);

  -----------------------------------------------------------------------------------------------------------
  -- 2. ORDERS & ORDER_ITEMS (SCD Type 2 + Partitioning)
  -----------------------------------------------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS `dataengineering-project-492505.silver_dataset.orders`
  (
      order_id INT64, customer_id INT64, order_date DATE, total_amount FLOAT64,
      updated_at TIMESTAMP, effective_start_date TIMESTAMP, effective_end_date TIMESTAMP, is_active BOOL
  ) PARTITION BY order_date CLUSTER BY order_id;

  MERGE `dataengineering-project-492505.silver_dataset.orders` T
  USING `dataengineering-project-492505.bronze_dataset.orders` S
  ON T.order_id = S.order_id AND T.is_active = TRUE
  WHEN MATCHED AND (T.total_amount != S.total_amount OR T.customer_id != S.customer_id) THEN
    UPDATE SET T.is_active = FALSE, T.effective_end_date = CURRENT_TIMESTAMP();

  INSERT INTO `dataengineering-project-492505.silver_dataset.orders`
  SELECT 
    order_id, customer_id, DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%z', order_date)), total_amount,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%z', updated_at), CURRENT_TIMESTAMP(), NULL, TRUE
  FROM `dataengineering-project-492505.bronze_dataset.orders` S
  WHERE NOT EXISTS (SELECT 1 FROM `dataengineering-project-492505.silver_dataset.orders` T WHERE T.order_id = S.order_id AND T.is_active = TRUE);

  -- Repeat logic for order_items
  CREATE TABLE IF NOT EXISTS `dataengineering-project-492505.silver_dataset.order_items`
  (
      order_item_id INT64, order_id INT64, product_id INT64, quantity INT64, price FLOAT64,
      updated_at TIMESTAMP, effective_start_date TIMESTAMP, effective_end_date TIMESTAMP, is_active BOOL
  ) CLUSTER BY order_id, product_id;

  MERGE `dataengineering-project-492505.silver_dataset.order_items` T
  USING `dataengineering-project-492505.bronze_dataset.order_items` S
  ON T.order_item_id = S.order_item_id AND T.is_active = TRUE
  WHEN MATCHED AND (T.quantity != S.quantity OR T.price != S.price) THEN
    UPDATE SET T.is_active = FALSE, T.effective_end_date = CURRENT_TIMESTAMP();

  INSERT INTO `dataengineering-project-492505.silver_dataset.order_items`
  SELECT 
    order_item_id, order_id, product_id, quantity, price,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%z', updated_at), CURRENT_TIMESTAMP(), NULL, TRUE
  FROM `dataengineering-project-492505.bronze_dataset.order_items` S
  WHERE NOT EXISTS (SELECT 1 FROM `dataengineering-project-492505.silver_dataset.order_items` T WHERE T.order_item_id = S.order_item_id AND T.is_active = TRUE);

  -----------------------------------------------------------------------------------------------------------
  -- 3. LOOKUP TABLES (Categories, Products, Suppliers - Atomic Overwrite)
  -----------------------------------------------------------------------------------------------------------
  CREATE OR REPLACE TABLE `dataengineering-project-492505.silver_dataset.categories` AS
  SELECT *, (category_id IS NULL OR name IS NULL) AS is_quarantined 
  FROM `dataengineering-project-492505.bronze_dataset.categories`;

  CREATE OR REPLACE TABLE `dataengineering-project-492505.silver_dataset.products` AS
  SELECT *, (product_id IS NULL OR name IS NULL) AS is_quarantined 
  FROM `dataengineering-project-492505.bronze_dataset.products`;

  CREATE OR REPLACE TABLE `dataengineering-project-492505.silver_dataset.suppliers` AS
  SELECT *, (supplier_id IS NULL OR supplier_name IS NULL) AS is_quarantined 
  FROM `dataengineering-project-492505.bronze_dataset.suppliers`;

  -----------------------------------------------------------------------------------------------------------
  -- 4. PRODUCT SUPPLIERS (SCD Type 2)
  -----------------------------------------------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS `dataengineering-project-492505.silver_dataset.product_suppliers`
  (
      supplier_id INT64, product_id INT64, supply_price FLOAT64, last_updated TIMESTAMP,
      effective_start_date TIMESTAMP, effective_end_date TIMESTAMP, is_active BOOL
  ) CLUSTER BY product_id, supplier_id;

  MERGE `dataengineering-project-492505.silver_dataset.product_suppliers` T
  USING `dataengineering-project-492505.bronze_dataset.product_suppliers` S
  ON T.supplier_id = S.supplier_id AND T.product_id = S.product_id AND T.is_active = TRUE
  WHEN MATCHED AND (T.supply_price != S.supply_price) THEN
    UPDATE SET T.is_active = FALSE, T.effective_end_date = CURRENT_TIMESTAMP();

  INSERT INTO `dataengineering-project-492505.silver_dataset.product_suppliers`
  SELECT 
    supplier_id, product_id, supply_price, SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%z', last_updated),
    CURRENT_TIMESTAMP(), NULL, TRUE
  FROM `dataengineering-project-492505.bronze_dataset.product_suppliers` S
  WHERE NOT EXISTS (SELECT 1 FROM `dataengineering-project-492505.silver_dataset.product_suppliers` T 
                    WHERE T.supplier_id = S.supplier_id AND T.product_id = S.product_id AND T.is_active = TRUE);

  -----------------------------------------------------------------------------------------------------------
  -- 5. CUSTOMER REVIEWS (SCD Type 2)
  -----------------------------------------------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS `dataengineering-project-492505.silver_dataset.customer_reviews`
  (
      id STRING, customer_id INT64, product_id INT64, rating INT64, review_text STRING, 
      review_date DATE, effective_start_date TIMESTAMP, effective_end_date TIMESTAMP, is_active BOOL
  ) CLUSTER BY product_id;

  MERGE `dataengineering-project-492505.silver_dataset.customer_reviews` T
  USING `dataengineering-project-492505.bronze_dataset.customer_reviews` S
  ON T.id = S.id AND T.is_active = TRUE
  WHEN MATCHED AND (T.rating != S.rating OR T.review_text != S.review_text) THEN
    UPDATE SET T.is_active = FALSE, T.effective_end_date = CURRENT_TIMESTAMP();

  INSERT INTO `dataengineering-project-492505.silver_dataset.customer_reviews`
  SELECT 
    id, customer_id, product_id, rating, review_text, DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%z', review_date)),
    CURRENT_TIMESTAMP(), NULL, TRUE
  FROM `dataengineering-project-492505.bronze_dataset.customer_reviews` S
  WHERE NOT EXISTS (SELECT 1 FROM `dataengineering-project-492505.silver_dataset.customer_reviews` T WHERE T.id = S.id AND T.is_active = TRUE);

END;