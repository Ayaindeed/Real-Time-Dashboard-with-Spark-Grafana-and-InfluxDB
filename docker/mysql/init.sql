-- MySQL initialization script for ecommerce database

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;

-- Create user_demographics table
CREATE TABLE IF NOT EXISTS user_demographics (
    id INT PRIMARY KEY,
    age INT NOT NULL,
    gender VARCHAR(10) NOT NULL,
    state VARCHAR(50) NOT NULL,
    country VARCHAR(10) NOT NULL,
    registration_date DATE,
    income_bracket VARCHAR(20),
    last_login DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_country ON user_demographics(country);
CREATE INDEX IF NOT EXISTS idx_age ON user_demographics(age);
CREATE INDEX IF NOT EXISTS idx_gender ON user_demographics(gender);
CREATE INDEX IF NOT EXISTS idx_registration_date ON user_demographics(registration_date);
CREATE INDEX IF NOT EXISTS idx_income_bracket ON user_demographics(income_bracket);

-- Create campaigns table for reference data
CREATE TABLE IF NOT EXISTS campaigns (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    type ENUM('seasonal', 'promotional', 'regular') DEFAULT 'regular',
    start_date DATE,
    end_date DATE,
    budget DECIMAL(10,2),
    target_audience VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert sample campaign data
INSERT IGNORE INTO campaigns (id, name, type, start_date, end_date, budget, target_audience) VALUES
('SUMMER2024', 'Summer Sale 2024', 'seasonal', '2024-06-01', '2024-08-31', 50000.00, 'All demographics'),
('WINTER2024', 'Winter Collection', 'seasonal', '2024-12-01', '2025-02-28', 75000.00, 'Cold climate regions'),
('SPRING2024', 'Spring Launch', 'seasonal', '2024-03-01', '2024-05-31', 40000.00, 'Young adults'),
('FALL2024', 'Fall Fashion', 'seasonal', '2024-09-01', '2024-11-30', 60000.00, 'Fashion conscious'),
('BLACKFRIDAY', 'Black Friday Deals', 'promotional', '2024-11-29', '2024-11-29', 100000.00, 'Bargain hunters'),
('CYBER_MONDAY', 'Cyber Monday Sale', 'promotional', '2024-12-02', '2024-12-02', 80000.00, 'Online shoppers');

-- Create products table for reference
CREATE TABLE IF NOT EXISTS products (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    description TEXT,
    brand VARCHAR(100),
    status ENUM('active', 'inactive', 'discontinued') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Create indexes for products
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);
CREATE INDEX IF NOT EXISTS idx_products_status ON products(status);

-- Insert sample product data
INSERT IGNORE INTO products (id, name, category, subcategory, price, cost, brand, status) VALUES
('PROD_1', 'Wireless Bluetooth Headphones', 'Electronics', 'Audio', 129.99, 65.00, 'TechBrand', 'active'),
('PROD_2', 'Organic Cotton T-Shirt', 'Clothing', 'Tops', 29.99, 12.00, 'EcoWear', 'active'),
('PROD_3', 'Smart Fitness Watch', 'Electronics', 'Wearables', 199.99, 95.00, 'FitTech', 'active'),
('PROD_4', 'Coffee Maker Deluxe', 'Home & Kitchen', 'Appliances', 89.99, 40.00, 'BrewMaster', 'active'),
('PROD_5', 'Running Shoes Pro', 'Sports', 'Footwear', 149.99, 70.00, 'SportMax', 'active');

-- Create event_summary table for aggregated data
CREATE TABLE IF NOT EXISTS event_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    hour INT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    campaign_id VARCHAR(50),
    country VARCHAR(10),
    total_events INT DEFAULT 0,
    unique_users INT DEFAULT 0,
    total_revenue DECIMAL(15,2) DEFAULT 0.00,
    avg_order_value DECIMAL(10,2) DEFAULT 0.00,
    conversion_rate DECIMAL(5,4) DEFAULT 0.0000,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_summary (date, hour, event_type, campaign_id, country)
);

-- Create indexes for event_summary
CREATE INDEX IF NOT EXISTS idx_event_summary_date ON event_summary(date);
CREATE INDEX IF NOT EXISTS idx_event_summary_hour ON event_summary(hour);
CREATE INDEX IF NOT EXISTS idx_event_summary_type ON event_summary(event_type);
CREATE INDEX IF NOT EXISTS idx_event_summary_campaign ON event_summary(campaign_id);
CREATE INDEX IF NOT EXISTS idx_event_summary_country ON event_summary(country);

-- Create stored procedures for common operations

-- Procedure to get user demographics by ID
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS GetUserDemographics(IN user_id INT)
BEGIN
    SELECT * FROM user_demographics WHERE id = user_id;
END //
DELIMITER ;

-- Procedure to get campaign performance
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS GetCampaignPerformance(IN campaign_id VARCHAR(50), IN start_date DATE, IN end_date DATE)
BEGIN
    SELECT 
        campaign_id,
        DATE(date) as summary_date,
        SUM(total_events) as total_events,
        SUM(unique_users) as total_unique_users,
        SUM(total_revenue) as total_revenue,
        AVG(conversion_rate) as avg_conversion_rate
    FROM event_summary
    WHERE campaign_id = campaign_id
      AND date BETWEEN start_date AND end_date
    GROUP BY campaign_id, DATE(date)
    ORDER BY summary_date;
END //
DELIMITER ;

-- Procedure to update event summary
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS UpdateEventSummary(
    IN p_date DATE,
    IN p_hour INT,
    IN p_event_type VARCHAR(50),
    IN p_campaign_id VARCHAR(50),
    IN p_country VARCHAR(10),
    IN p_total_events INT,
    IN p_unique_users INT,
    IN p_total_revenue DECIMAL(15,2),
    IN p_avg_order_value DECIMAL(10,2),
    IN p_conversion_rate DECIMAL(5,4)
)
BEGIN
    INSERT INTO event_summary (
        date, hour, event_type, campaign_id, country,
        total_events, unique_users, total_revenue, avg_order_value, conversion_rate
    )
    VALUES (
        p_date, p_hour, p_event_type, p_campaign_id, p_country,
        p_total_events, p_unique_users, p_total_revenue, p_avg_order_value, p_conversion_rate
    )
    ON DUPLICATE KEY UPDATE
        total_events = total_events + p_total_events,
        unique_users = GREATEST(unique_users, p_unique_users),
        total_revenue = total_revenue + p_total_revenue,
        avg_order_value = (total_revenue + p_total_revenue) / GREATEST(total_events + p_total_events, 1),
        conversion_rate = p_conversion_rate;
END //
DELIMITER ;

-- Create views for common queries

-- View for user demographics with age groups
CREATE OR REPLACE VIEW user_demographics_grouped AS
SELECT 
    id,
    age,
    CASE 
        WHEN age < 25 THEN '18-24'
        WHEN age < 35 THEN '25-34'
        WHEN age < 45 THEN '35-44'
        WHEN age < 55 THEN '45-54'
        ELSE '55+'
    END AS age_group,
    gender,
    state,
    country,
    CASE 
        WHEN country IN ('US', 'CA') THEN 'North America'
        WHEN country IN ('UK', 'DE', 'FR') THEN 'Europe'
        WHEN country IN ('JP', 'IN') THEN 'Asia'
        WHEN country = 'AU' THEN 'Oceania'
        ELSE 'Other'
    END AS region,
    registration_date,
    income_bracket,
    last_login,
    DATEDIFF(CURDATE(), registration_date) AS days_since_registration
FROM user_demographics;

-- View for campaign summary
CREATE OR REPLACE VIEW campaign_summary AS
SELECT 
    c.id,
    c.name,
    c.type,
    c.start_date,
    c.end_date,
    c.budget,
    c.target_audience,
    COALESCE(SUM(es.total_events), 0) AS total_events,
    COALESCE(SUM(es.total_revenue), 0) AS total_revenue,
    COALESCE(AVG(es.conversion_rate), 0) AS avg_conversion_rate
FROM campaigns c
LEFT JOIN event_summary es ON c.id = es.campaign_id
GROUP BY c.id, c.name, c.type, c.start_date, c.end_date, c.budget, c.target_audience;

-- Grant permissions to application user
GRANT SELECT, INSERT, UPDATE, DELETE ON ecommerce.* TO 'user'@'%';
GRANT EXECUTE ON ecommerce.* TO 'user'@'%';

-- Optimize MySQL settings for this workload
SET GLOBAL innodb_buffer_pool_size = 128M;
SET GLOBAL max_connections = 100;
SET GLOBAL query_cache_type = ON;
SET GLOBAL query_cache_size = 16M;

-- Create initial admin user record for testing
INSERT IGNORE INTO user_demographics (id, age, gender, state, country, registration_date, income_bracket, last_login) 
VALUES (0, 30, 'Other', 'Admin', 'US', CURDATE(), '100k+', CURDATE());

-- Log initialization completion
INSERT INTO event_summary (date, hour, event_type, campaign_id, country, total_events, unique_users, total_revenue)
VALUES (CURDATE(), HOUR(NOW()), 'system_init', 'SYSTEM', 'US', 1, 1, 0.00);

-- Display initialization summary
SELECT 'Database initialization completed successfully' AS status,
       COUNT(*) AS demographics_table_exists FROM information_schema.tables WHERE table_schema = 'ecommerce' AND table_name = 'user_demographics';

SELECT 'Sample data inserted' AS status,
       (SELECT COUNT(*) FROM campaigns) AS campaign_count,
       (SELECT COUNT(*) FROM products) AS product_count;