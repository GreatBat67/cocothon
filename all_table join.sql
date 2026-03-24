CREATE OR REPLACE TABLE SNOWFLAKE_LEARNING_DB.PUBLIC.seller_order_customer_metrics AS
SELECT
    -- Order Details
    o.order_id,
    o.order_date,
    o.product_id,
    o.product_name,
    o.brand,
    o.category,
    o.sub_category,
    o.quantity,
    o.unit_price,
    o.order_value,
    o.discount_amount,
    o.payment_type,
    o.payment_status,
    o.order_status,
    o.delivery_partner,
    o.delivery_time_days,
    o.is_returned,
    o.is_cancelled,
    o.cod_flag,
    o.customer_rating AS order_rating,
    o.high_rto_risk_flag,
    o.fraud_flag,
    o.margin,
    o.profit,
    
    -- Customer Details
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.gender,
    c.age,
    c.city AS customer_city,
    c.state AS customer_state,
    c.loyalty_tier,
    c.loyalty_points,
    c.customer_type,
    c.preferred_payment_method,
    c.preferred_category,
    c.total_orders AS customer_total_orders,
    c.customer_lifetime_value,
    c.discount_sensitivity_score,
    c.price_sensitivity_score,
    c.return_behavior_score,
    c.fraud_risk_score AS customer_fraud_risk,
    c.churn_risk_score,
    c.engagement_score,
    
    -- Seller Details
    s.seller_id,
    s.seller_name,
    s.seller_type,
    s.seller_category,
    s.seller_tier,
    s.seller_city,
    s.seller_state,
    s.years_on_platform,
    s.total_orders AS seller_total_orders,
    s.total_revenue AS seller_total_revenue,
    s.avg_order_value AS seller_avg_order_value,
    s.growth_rate_pct,
    s.repeat_customer_pct,
    s.avg_customer_rating AS seller_avg_rating,
    s.return_rate_pct AS seller_return_rate,
    s.cancellation_rate_pct AS seller_cancel_rate,
    s.rto_rate_pct AS seller_rto_rate,
    s.fulfillment_type,
    s.top_delivery_partner AS seller_top_partner,
    s.marketing_roi,
    s.seller_score,
    s.seller_risk_score,
    s.is_top_seller,
    s.is_high_return_seller,
    s.is_high_risk_seller

FROM SNOWFLAKE_LEARNING_DB.PUBLIC.orders o
JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.customers c ON o.customer_id = c.customer_id
JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.customer_seller_mapping m ON o.order_id = m.order_id
JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.sellers s ON m.seller_id = s.seller_id;



---seller gold table ----
CREATE OR REPLACE transient TABLE SNOWFLAKE_LEARNING_DB.PUBLIC.gold_seller_360 AS
WITH order_metrics AS (
    SELECT
        m.seller_id,
        COUNT(*) AS total_orders_actual,
        COUNT(DISTINCT o.order_id) AS unique_orders,
        COUNT(DISTINCT o.customer_id) AS unique_customers,
        SUM(o.order_value) AS total_order_value,
        SUM(o.discount_amount) AS total_discount_given,
        AVG(o.order_value) AS avg_order_value,
        MAX(o.order_value) AS max_order_value,
        MIN(o.order_value) AS min_order_value,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        DATEDIFF(DAY, MIN(o.order_date), MAX(o.order_date)) AS order_span_days,
        SUM(o.quantity) AS total_items_sold,
        AVG(o.quantity) AS avg_items_per_order,
        COUNT(DISTINCT o.category) AS unique_categories_sold,
        COUNT(DISTINCT o.brand) AS unique_brands_sold,
        COUNT(DISTINCT o.product_id) AS unique_products_sold,
        SUM(CASE WHEN o.is_returned THEN 1 ELSE 0 END) AS total_returns,
        SUM(CASE WHEN o.is_cancelled THEN 1 ELSE 0 END) AS total_cancellations,
        SUM(o.refund_amount) AS total_refund_amount,
        ROUND(SUM(CASE WHEN o.is_returned THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS actual_return_rate_pct,
        ROUND(SUM(CASE WHEN o.is_cancelled THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS actual_cancel_rate_pct,
        SUM(CASE WHEN o.cod_flag THEN 1 ELSE 0 END) AS cod_orders,
        SUM(CASE WHEN o.payment_type = 'UPI' THEN 1 ELSE 0 END) AS upi_orders,
        SUM(CASE WHEN o.payment_type IN ('Credit Card', 'Debit Card') THEN 1 ELSE 0 END) AS card_orders,
        SUM(CASE WHEN o.payment_type = 'Wallet' THEN 1 ELSE 0 END) AS wallet_orders,
        ROUND(SUM(CASE WHEN o.cod_flag THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS cod_pct,
        SUM(CASE WHEN o.order_status = 'Delivered' THEN 1 ELSE 0 END) AS delivered_orders,
        AVG(o.delivery_time_days) AS avg_delivery_days_actual,
        AVG(o.customer_rating) AS avg_customer_rating_actual,
        SUM(CASE WHEN o.high_rto_risk_flag THEN 1 ELSE 0 END) AS high_rto_risk_orders,
        SUM(CASE WHEN o.fraud_flag THEN 1 ELSE 0 END) AS fraud_flagged_orders,
        SUM(o.margin) AS total_margin,
        SUM(o.profit) AS total_profit,
        ROUND(AVG(o.margin), 2) AS avg_margin_pct,
        ROUND(SUM(o.profit) / NULLIF(SUM(o.order_value), 0) * 100, 2) AS profit_margin_pct
    FROM SNOWFLAKE_LEARNING_DB.PUBLIC.orders o
    JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.customer_seller_mapping m ON o.order_id = m.order_id
    GROUP BY m.seller_id
),
customer_metrics AS (
    SELECT
        m.seller_id,
        COUNT(DISTINCT c.customer_id) AS total_customers_served,
        AVG(c.customer_lifetime_value) AS avg_customer_clv,
        AVG(c.churn_risk_score) AS avg_customer_churn_risk,
        AVG(c.engagement_score) AS avg_customer_engagement,
        AVG(c.fraud_risk_score) AS avg_customer_fraud_risk,
        AVG(c.discount_sensitivity_score) AS avg_discount_sensitivity,
        AVG(c.price_sensitivity_score) AS avg_price_sensitivity,
        SUM(CASE WHEN c.loyalty_tier = 'Platinum' THEN 1 ELSE 0 END) AS platinum_customers,
        SUM(CASE WHEN c.loyalty_tier = 'Gold' THEN 1 ELSE 0 END) AS gold_customers,
        SUM(CASE WHEN c.loyalty_tier = 'Silver' THEN 1 ELSE 0 END) AS silver_customers,
        SUM(CASE WHEN c.loyalty_tier = 'Bronze' THEN 1 ELSE 0 END) AS bronze_customers,
        ROUND(SUM(CASE WHEN c.loyalty_tier IN ('Platinum', 'Gold') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(DISTINCT c.customer_id), 0), 2) AS high_value_customer_pct_actual
    FROM SNOWFLAKE_LEARNING_DB.PUBLIC.customers c
    JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.customer_seller_mapping m ON c.customer_id = m.customer_id
    GROUP BY m.seller_id
),
top_categories AS (
    SELECT seller_id, category AS most_sold_category
    FROM (
        SELECT m.seller_id, o.category, COUNT(*) AS cnt,
               ROW_NUMBER() OVER (PARTITION BY m.seller_id ORDER BY COUNT(*) DESC) AS rn
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.orders o
        JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.customer_seller_mapping m ON o.order_id = m.order_id
        GROUP BY m.seller_id, o.category
    ) WHERE rn = 1
),
top_brands AS (
    SELECT seller_id, brand AS most_sold_brand
    FROM (
        SELECT m.seller_id, o.brand, COUNT(*) AS cnt,
               ROW_NUMBER() OVER (PARTITION BY m.seller_id ORDER BY COUNT(*) DESC) AS rn
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.orders o
        JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.customer_seller_mapping m ON o.order_id = m.order_id
        GROUP BY m.seller_id, o.brand
    ) WHERE rn = 1
),
top_payment AS (
    SELECT seller_id, payment_type AS preferred_payment_type
    FROM (
        SELECT m.seller_id, o.payment_type, COUNT(*) AS cnt,
               ROW_NUMBER() OVER (PARTITION BY m.seller_id ORDER BY COUNT(*) DESC) AS rn
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.orders o
        JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.customer_seller_mapping m ON o.order_id = m.order_id
        GROUP BY m.seller_id, o.payment_type
    ) WHERE rn = 1
)
SELECT
    -- Seller Profile
    s.seller_id,
    s.seller_name,
    s.seller_type,
    s.seller_category,
    s.seller_tier,
    s.seller_city,
    s.seller_state,
    s.seller_pincode,
    s.seller_country,
    s.seller_onboard_date,
    s.years_on_platform,
    DATEDIFF(DAY, s.seller_onboard_date, CURRENT_DATE()) AS seller_tenure_days,
    
    -- Product Metrics
    s.total_products_listed,
    s.active_products,
    ROUND(s.active_products * 100.0 / NULLIF(s.total_products_listed, 0), 2) AS product_active_pct,
    
    -- Order Metrics (Profile)
    s.total_orders AS profile_total_orders,
    s.total_revenue AS profile_total_revenue,
    s.avg_order_value AS profile_avg_order_value,
    s.monthly_sales AS profile_monthly_sales,
    s.growth_rate_pct,
    
    -- Order Metrics (Actual from Orders)
    om.total_orders_actual,
    om.unique_orders,
    om.unique_customers,
    om.total_order_value,
    om.total_discount_given,
    om.avg_order_value,
    om.max_order_value,
    om.min_order_value,
    om.first_order_date,
    om.last_order_date,
    om.order_span_days,
    om.total_items_sold,
    om.avg_items_per_order,
    om.unique_categories_sold,
    om.unique_brands_sold,
    om.unique_products_sold,
    
    -- Returns & Cancellations
    om.total_returns,
    om.total_cancellations,
    om.total_refund_amount,
    om.actual_return_rate_pct,
    om.actual_cancel_rate_pct,
    s.return_rate_pct AS profile_return_rate,
    s.cancellation_rate_pct AS profile_cancel_rate,
    s.rto_rate_pct,
    s.fraud_flag_rate,
    
    -- Payment Metrics
    om.cod_orders,
    om.upi_orders,
    om.card_orders,
    om.wallet_orders,
    om.cod_pct,
    tp.preferred_payment_type,
    
    -- Delivery Metrics
    om.delivered_orders,
    om.avg_delivery_days_actual,
    s.avg_delivery_days AS profile_avg_delivery_days,
    s.late_delivery_pct,
    s.fulfillment_type,
    s.top_delivery_partner,
    
    -- Customer Rating & Risk
    om.avg_customer_rating_actual,
    s.avg_customer_rating AS profile_avg_rating,
    om.high_rto_risk_orders,
    om.fraud_flagged_orders,
    ROUND(om.fraud_flagged_orders * 100.0 / NULLIF(om.total_orders_actual, 0), 2) AS fraud_rate_pct,
    
    -- Profitability
    om.total_margin,
    om.total_profit,
    om.avg_margin_pct,
    om.profit_margin_pct,
    
    -- Customer Metrics
    s.unique_customers_served AS profile_unique_customers,
    s.repeat_customer_pct,
    s.customer_lifetime_value_avg AS profile_customer_clv,
    cm.total_customers_served,
    cm.avg_customer_clv,
    cm.avg_customer_churn_risk,
    cm.avg_customer_engagement,
    cm.avg_customer_fraud_risk,
    cm.avg_discount_sensitivity,
    cm.avg_price_sensitivity,
    cm.platinum_customers,
    cm.gold_customers,
    cm.silver_customers,
    cm.bronze_customers,
    cm.high_value_customer_pct_actual,
    s.high_value_customer_pct AS profile_high_value_pct,
    s.discount_hunter_pct,
    s.cod_users_pct,
    s.high_risk_customer_pct,
    
    -- Inventory Metrics
    s.stock_out_rate,
    s.inventory_turnover_ratio,
    s.avg_stock_level,
    
    -- Marketing Metrics
    s.marketing_spend,
    s.marketing_roi,
    s.conversion_rate_pct,
    s.click_through_rate_pct,
    s.avg_discount_pct,
    s.price_competitiveness_score,
    s.campaigns_run,
    s.email_engagement_rate,
    s.whatsapp_engagement_rate,
    
    -- Top Performers
    tc.most_sold_category,
    tb.most_sold_brand,
    
    -- Scores & Flags
    s.seller_score,
    s.seller_risk_score,
    s.seller_growth_score,
    s.is_top_seller,
    s.is_high_return_seller,
    s.is_high_risk_seller,
    
    -- Derived Risk Segment
    CASE 
        WHEN s.seller_risk_score >= 70 OR s.is_high_risk_seller THEN 'High Risk'
        WHEN s.seller_risk_score >= 40 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS risk_segment,
    
    -- Derived Performance Segment
    CASE 
        WHEN s.seller_score >= 80 AND s.is_top_seller THEN 'Top Performer'
        WHEN s.seller_score >= 60 THEN 'Good Performer'
        WHEN s.seller_score >= 40 THEN 'Average Performer'
        ELSE 'Needs Improvement'
    END AS performance_segment,
    
    -- Derived Growth Segment
    CASE 
        WHEN s.growth_rate_pct >= 30 THEN 'High Growth'
        WHEN s.growth_rate_pct >= 10 THEN 'Moderate Growth'
        WHEN s.growth_rate_pct >= 0 THEN 'Stable'
        ELSE 'Declining'
    END AS growth_segment,
    
    CURRENT_TIMESTAMP() AS created_at

FROM SNOWFLAKE_LEARNING_DB.PUBLIC.sellers s
LEFT JOIN order_metrics om ON s.seller_id = om.seller_id
LEFT JOIN customer_metrics cm ON s.seller_id = cm.seller_id
LEFT JOIN top_categories tc ON s.seller_id = tc.seller_id
LEFT JOIN top_brands tb ON s.seller_id = tb.seller_id
LEFT JOIN top_payment tp ON s.seller_id = tp.seller_id;


