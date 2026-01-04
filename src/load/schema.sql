-- LogiFlow ETL Pipeline - Database Schema
-- =========================================
-- Designed for PostgreSQL, compatible with SQLite
-- 
-- This schema mirrors fleet management data patterns:
-- - orders → trips/deliveries
-- - customers → clients/recipients
-- - sellers → warehouses/drivers
-- - order_items → shipment details

-- ============================================
-- CLEAN SLATE (for development/testing only)
-- ============================================
-- DROP TABLE IF EXISTS order_items CASCADE;
-- DROP TABLE IF EXISTS orders CASCADE;
-- DROP TABLE IF EXISTS products CASCADE;
-- DROP TABLE IF EXISTS customers CASCADE;
-- DROP TABLE IF EXISTS sellers CASCADE;

-- ============================================
-- CUSTOMERS TABLE
-- Similar to: Fleet clients, delivery recipients
-- ============================================
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10),
    customer_zip_code VARCHAR(20),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for geographic queries (relevant for fleet routing)
CREATE INDEX IF NOT EXISTS idx_customers_location 
ON customers(customer_state, customer_city);


-- ============================================
-- SELLERS TABLE
-- Similar to: Warehouses, distribution hubs, drivers
-- ============================================
CREATE TABLE IF NOT EXISTS sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_city VARCHAR(100),
    seller_state VARCHAR(10),
    seller_zip_code VARCHAR(20),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for hub location queries
CREATE INDEX IF NOT EXISTS idx_sellers_location 
ON sellers(seller_state, seller_city);


-- ============================================
-- PRODUCTS TABLE
-- Similar to: Vehicle types, cargo categories
-- ============================================
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(500),
    description TEXT,
    category VARCHAR(100),
    price DECIMAL(10, 2),
    rating_rate DECIMAL(3, 2),
    rating_count INTEGER,
    
    -- Metadata
    source VARCHAR(50),
    extracted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for category filtering
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);


-- ============================================
-- ORDERS TABLE
-- Similar to: Trips, deliveries, routes
-- ============================================
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(30),
    
    -- Timestamps (critical for fleet management!)
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    
    -- Derived metrics
    delivery_duration_hours DECIMAL(10, 2),
    
    -- Metadata
    source VARCHAR(50),
    extracted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key (optional, may not exist for API data)
    CONSTRAINT fk_orders_customer 
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        ON DELETE SET NULL
);

-- Primary index for time-series queries (essential for fleet analytics)
CREATE INDEX IF NOT EXISTS idx_orders_purchase_timestamp 
ON orders(order_purchase_timestamp);

-- Index for status filtering
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(order_status);

-- Index for customer order history
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);


-- ============================================
-- ORDER ITEMS TABLE
-- Similar to: Shipment details, cargo items
-- ============================================
CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,  -- Auto-increment for SQLite compatibility
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    
    -- Item details
    order_item_id INTEGER,
    quantity INTEGER DEFAULT 1,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    
    -- Derived metrics
    shipping_cost_ratio DECIMAL(6, 4),
    
    -- Shipping timeline
    shipping_limit_date TIMESTAMP,
    
    -- Metadata
    source VARCHAR(50),
    extracted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign keys
    CONSTRAINT fk_order_items_order 
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
        ON DELETE CASCADE,
    
    CONSTRAINT fk_order_items_product 
        FOREIGN KEY (product_id) REFERENCES products(product_id)
        ON DELETE SET NULL,
    
    CONSTRAINT fk_order_items_seller 
        FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
        ON DELETE SET NULL
);

-- Composite index for order lookups
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_order_items_seller ON order_items(seller_id);


-- ============================================
-- DATA QUALITY TRACKING TABLE
-- For monitoring ETL pipeline health
-- ============================================
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name VARCHAR(50) NOT NULL,
    source VARCHAR(50),
    rows_extracted INTEGER,
    rows_transformed INTEGER,
    rows_loaded INTEGER,
    validation_passed BOOLEAN,
    validation_errors TEXT,
    duration_seconds DECIMAL(10, 2),
    status VARCHAR(20) DEFAULT 'running'
);

CREATE INDEX IF NOT EXISTS idx_etl_log_timestamp ON etl_run_log(run_timestamp);
CREATE INDEX IF NOT EXISTS idx_etl_log_table ON etl_run_log(table_name);


-- ============================================
-- USEFUL VIEWS FOR ANALYTICS
-- ============================================

-- View: Order summary with delivery performance
CREATE VIEW IF NOT EXISTS v_order_summary AS
SELECT 
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.delivery_duration_hours,
    c.customer_city,
    c.customer_state,
    COUNT(oi.id) as item_count,
    SUM(oi.price) as total_price,
    SUM(oi.freight_value) as total_freight
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY 
    o.order_id, o.order_status, o.order_purchase_timestamp,
    o.order_delivered_customer_date, o.delivery_duration_hours,
    c.customer_city, c.customer_state;


-- View: Delivery performance by state
CREATE VIEW IF NOT EXISTS v_delivery_performance AS
SELECT 
    c.customer_state,
    COUNT(o.order_id) as total_orders,
    AVG(o.delivery_duration_hours) as avg_delivery_hours,
    SUM(CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END) as delivered_count,
    SUM(CASE WHEN o.order_status = 'canceled' THEN 1 ELSE 0 END) as canceled_count
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_state;
