 --add seller things too
CREATE OR REPLACE TRANSIENT TABLE seller_json_raw AS
SELECT SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Generate 200 rows of realistic ecommerce SELLER data in JSON array format.

Each seller should represent a marketplace vendor (like Amazon/Flipkart sellers) and should be logically connected to customers and orders.

Columns:

-- Seller Identity
seller_id (int, unique),
seller_name,
seller_type (brand, reseller, manufacturer, dropshipper),
seller_category (electronics, fashion, home, beauty, etc),
seller_tier (bronze, silver, gold, platinum),

-- Location
seller_city,
seller_state,
seller_pincode,
seller_country,

-- Business Profile
seller_onboard_date,
years_on_platform (int),
total_products_listed (int),
active_products (int),

-- Sales Performance
total_orders (int),
total_revenue (float),
avg_order_value (float),
monthly_sales (float),
growth_rate_pct (float),

-- Customer Metrics (IMPORTANT LINK)
unique_customers_served (int),
repeat_customer_pct (float),
avg_customer_rating (float),
customer_lifetime_value_avg (float),

-- Risk & Quality
return_rate_pct (float),
cancellation_rate_pct (float),
rto_rate_pct (float),
fraud_flag_rate (float),

-- Logistics Performance
avg_delivery_days (float),
late_delivery_pct (float),
fulfillment_type (self, warehouse, hybrid),
top_delivery_partner,

-- Inventory
stock_out_rate (float),
inventory_turnover_ratio (float),
avg_stock_level (int),

-- Marketing Performance
marketing_spend (float),
marketing_roi (float),
conversion_rate_pct (float),
click_through_rate_pct (float),

-- Pricing & Discounts
avg_discount_pct (float),
price_competitiveness_score (float),

-- Engagement
campaigns_run (int),
email_engagement_rate (float),
whatsapp_engagement_rate (float),

-- Customer Persona Mix (VERY IMPORTANT)
high_value_customer_pct (float),
discount_hunter_pct (float),
cod_users_pct (float),
high_risk_customer_pct (float),

-- Derived Scores
seller_score (float),
seller_risk_score (float),
seller_growth_score (float),

-- Flags
is_top_seller (true/false),
is_high_return_seller (true/false),
is_high_risk_seller (true/false),

Make data realistic:
- Some sellers should be high-performing
- Some should have high RTO / fraud
- Some should be premium sellers
- Maintain diversity in categories and performance

Ensure values are consistent (e.g., high revenue → high orders)

Return ONLY JSON array. No explanation.'
) AS json_data;

select count(*) from  SELLER_JSON_RAW;


CREATE OR REPLACE TABLE sellers (
seller_id INT,
seller_name STRING,
seller_type STRING,
seller_category STRING,
seller_tier STRING,

seller_city STRING,
seller_state STRING,
seller_pincode STRING,
seller_country STRING,

seller_onboard_date DATE,
years_on_platform INT,
total_products_listed INT,
active_products INT,

total_orders INT,
total_revenue FLOAT,
avg_order_value FLOAT,
monthly_sales FLOAT,
growth_rate_pct FLOAT,

unique_customers_served INT,
repeat_customer_pct FLOAT,
avg_customer_rating FLOAT,
customer_lifetime_value_avg FLOAT,

return_rate_pct FLOAT,
cancellation_rate_pct FLOAT,
rto_rate_pct FLOAT,
fraud_flag_rate FLOAT,

avg_delivery_days FLOAT,
late_delivery_pct FLOAT,
fulfillment_type STRING,
top_delivery_partner STRING,

stock_out_rate FLOAT,
inventory_turnover_ratio FLOAT,
avg_stock_level INT,

marketing_spend FLOAT,
marketing_roi FLOAT,
conversion_rate_pct FLOAT,
click_through_rate_pct FLOAT,

avg_discount_pct FLOAT,
price_competitiveness_score FLOAT,

campaigns_run INT,
email_engagement_rate FLOAT,
whatsapp_engagement_rate FLOAT,

high_value_customer_pct FLOAT,
discount_hunter_pct FLOAT,
cod_users_pct FLOAT,
high_risk_customer_pct FLOAT,

seller_score FLOAT,
seller_risk_score FLOAT,
seller_growth_score FLOAT,

is_top_seller BOOLEAN,
is_high_return_seller BOOLEAN,
is_high_risk_seller BOOLEAN
);


INSERT INTO sellers
SELECT
f.value:seller_id::INT,
f.value:seller_name::STRING,
f.value:seller_type::STRING,
f.value:seller_category::STRING,
f.value:seller_tier::STRING,

f.value:seller_city::STRING,
f.value:seller_state::STRING,
f.value:seller_pincode::STRING,
f.value:seller_country::STRING,

f.value:seller_onboard_date::DATE,
f.value:years_on_platform::INT,
f.value:total_products_listed::INT,
f.value:active_products::INT,

f.value:total_orders::INT,
f.value:total_revenue::FLOAT,
f.value:avg_order_value::FLOAT,
f.value:monthly_sales::FLOAT,
f.value:growth_rate_pct::FLOAT,

f.value:unique_customers_served::INT,
f.value:repeat_customer_pct::FLOAT,
f.value:avg_customer_rating::FLOAT,
f.value:customer_lifetime_value_avg::FLOAT,

f.value:return_rate_pct::FLOAT,
f.value:cancellation_rate_pct::FLOAT,
f.value:rto_rate_pct::FLOAT,
f.value:fraud_flag_rate::FLOAT,

f.value:avg_delivery_days::FLOAT,
f.value:late_delivery_pct::FLOAT,
f.value:fulfillment_type::STRING,
f.value:top_delivery_partner::STRING,

f.value:stock_out_rate::FLOAT,
f.value:inventory_turnover_ratio::FLOAT,
f.value:avg_stock_level::INT,

f.value:marketing_spend::FLOAT,
f.value:marketing_roi::FLOAT,
f.value:conversion_rate_pct::FLOAT,
f.value:click_through_rate_pct::FLOAT,

f.value:avg_discount_pct::FLOAT,
f.value:price_competitiveness_score::FLOAT,

f.value:campaigns_run::INT,
f.value:email_engagement_rate::FLOAT,
f.value:whatsapp_engagement_rate::FLOAT,

f.value:high_value_customer_pct::FLOAT,
f.value:discount_hunter_pct::FLOAT,
f.value:cod_users_pct::FLOAT,
f.value:high_risk_customer_pct::FLOAT,

f.value:seller_score::FLOAT,
f.value:seller_risk_score::FLOAT,
f.value:seller_growth_score::FLOAT,

f.value:is_top_seller::BOOLEAN,
f.value:is_high_return_seller::BOOLEAN,
f.value:is_high_risk_seller::BOOLEAN

FROM seller_json_raw,
LATERAL FLATTEN(input => TRY_PARSE_JSON(json_data)) f
WHERE f.value IS NOT NULL;

select * from sellers;


--mapping
CREATE OR REPLACE TABLE customer_seller_mapping AS
SELECT
c.customer_id,
o.order_id,
o.product_id,
o.category,

/* Assign seller randomly but logically */
MOD(o.order_id, 200) + 1 AS seller_id

FROM SNOWFLAKE_LEARNING_DB.PUBLIC.orders o
JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.customers c
ON o.customer_id = c.customer_id;


select * from customer_seller_mapping;