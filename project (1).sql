---------------- cc-------------

CREATE OR REPLACE transient TABLE cortex_identity_insights AS
SELECT
    customer_id,

    SNOWFLAKE.CORTEX.COMPLETE(
    'snowflake-arctic',
    'Analyze this customer for fraud risk and explain in one line: ' ||
    'fraud_score=' || fraud_risk_score ||
    ', cod_pct=' || cod_pct ||
    ', return_rate=' || return_rate_pct ||
    ', cancellations=' || total_cancellations
    ) AS fraud_reason,

    SNOWFLAKE.CORTEX.COMPLETE(
    'snowflake-arctic',
    'Classify this customer into risk category (LOW, MEDIUM, HIGH): ' ||
    'fraud_score=' || fraud_risk_score ||
    ', cod_pct=' || cod_pct ||
    ', return_rate=' || return_rate_pct
    ) AS fraud_risk_label

FROM SNOWFLAKE_LEARNING_DB.PUBLIC.gold_customer_360;



select top 100 * from cortex_identity_insights;

--- ai table --

CREATE OR REPLACE transient TABLE SNOWFLAKE_LEARNING_DB.PUBLIC.customer_ai_360 AS
SELECT
g.*,

-- PERSONA 
SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Classify this customer into persona: Premium, Discount Hunter, Risky COD Buyer, Loyal Shopper. ' ||
'aov=' || avg_order_value ||
', orders=' || total_orders ||
', cod_pct=' || cod_pct ||
', discount=' || discount_sensitivity_score
) AS persona,

-- FRAUD / IDENTITY
SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Classify fraud risk (LOW, MEDIUM, HIGH): ' ||
'fraud_score=' || fraud_risk_score ||
', cod_pct=' || cod_pct ||
', return_rate=' || return_rate_pct
) AS fraud_risk_label,

SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Explain fraud risk in one short line: ' ||
'fraud_score=' || fraud_risk_score ||
', cod_pct=' || cod_pct ||
', return_rate=' || return_rate_pct
) AS fraud_reason,


--- RTO / PREDICTOR
SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Give settlement probability between 0 and 1: ' ||
'cod_pct=' || cod_pct ||
', return_rate=' || return_rate_pct ||
', cancellations=' || total_cancellations
) AS settlement_probability,

SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Classify RTO risk (LOW, MEDIUM, HIGH): ' ||
'cod_pct=' || cod_pct ||
', return_rate=' || return_rate_pct
) AS rto_risk_label,

-- MARKETING / OPTIMIZATION
SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Suggest marketing strategy in one line: ' ||
'roi=' || marketing_roi ||
', engagement=' || avg_engagement_score ||
', fatigue=' || avg_fatigue_score
) AS marketing_strategy,

SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Should we target aggressively, moderately, or avoid: ' ||
'roi=' || marketing_roi ||
', engagement=' || avg_engagement_score ||
', fatigue=' || avg_fatigue_score
) AS targeting_decision,


/* Logistics + Business Actions */

-- Payment
CASE
    WHEN cod_pct > 70 AND return_rate_pct > 25 THEN 'BLOCK_COD'
    WHEN cod_pct > 60 AND return_rate_pct > 20 THEN 'LIMIT_COD'
    WHEN cod_pct > 40 THEN 'ENABLE_PREPAID_DISCOUNT'
    ELSE 'ALLOW_ALL_PAYMENTS'
END AS payment_action,

-- Delivery
CASE
    WHEN avg_delivery_days > 7 AND return_rate_pct > 20 THEN 'USE_FASTEST_COURIER'
    WHEN avg_delivery_days > 5 THEN 'PRIORITIZE_EXPRESS_SHIPPING'
    ELSE 'STANDARD_DELIVERY'
END AS delivery_action,

-- Inventory
CASE
    WHEN total_orders > 50 THEN 'PRIORITY_STOCK'
    WHEN total_orders = 0 THEN 'LOW_PRIORITY'
    ELSE 'NORMAL'
END AS inventory_action,

-- Marketing
CASE
    WHEN marketing_roi < 0 THEN 'STOP_SPEND'
    WHEN avg_fatigue_score > 0.8 THEN 'COOLDOWN'
    WHEN avg_engagement_score > 0.7 THEN 'BOOST'
    ELSE 'OPTIMIZE'
END AS marketing_action,

-- CX
CASE
    WHEN total_cancellations > 5 THEN 'PROACTIVE_SUPPORT'
    WHEN avg_customer_rating < 3 THEN 'FEEDBACK_LOOP'
    ELSE 'STANDARD'
END AS cx_action,

-- Fraud
CASE
    WHEN fraud_risk_score > 0.4 THEN 'REVIEW'
    WHEN fraud_flagged_orders > 2 THEN 'BLOCK'
    ELSE 'MONITOR'
END AS fraud_action,

-- Lifecycle
CASE
    WHEN churn_risk_score > 0.7 THEN 'WINBACK'
    WHEN total_orders > 50 THEN 'REWARD'
    ELSE 'ENGAGE'
END AS lifecycle_action,

-- Personalization
CASE
    WHEN most_frequent_category IS NOT NULL THEN 
        'RECOMMEND_' || most_frequent_category
    ELSE 'GENERIC'
END AS personalization_action,

CURRENT_TIMESTAMP() AS ai_enriched_at

FROM SNOWFLAKE_LEARNING_DB.PUBLIC.gold_customer_360 g;

select top 100 * from SNOWFLAKE_LEARNING_DB.PUBLIC.customer_ai_360;

-----adding another layer of rfm scoring - 

CREATE OR REPLACE transient TABLE rfm_base AS
SELECT
customer_id,
--rfm here
DATEDIFF(DAY, last_order_date, CURRENT_DATE()) AS recency_days,
total_orders AS frequency,
total_order_value AS monetary_value,
--add avg

FROM SNOWFLAKE_LEARNING_DB.PUBLIC.gold_customer_360;

CREATE OR REPLACE transient TABLE rfm_scores AS
SELECT
customer_id,

/* Recency: lower is better → reverse score */
6 - NTILE(5) OVER (ORDER BY recency_days ASC) AS r_score,

/* Frequency: higher is better */
NTILE(5) OVER (ORDER BY frequency DESC) AS f_score,

/* Monetary: higher is better */
NTILE(5) OVER (ORDER BY monetary_value DESC) AS m_score

FROM rfm_base;

CREATE OR REPLACE transient TABLE rfm_final AS
SELECT
customer_id,
r_score,
f_score,
m_score,
(r_score + f_score + m_score) AS rfm_score
FROM rfm_scores;

CREATE OR REPLACE transient TABLE rfm_segments AS
SELECT
customer_id,
r_score,
f_score,
m_score,
rfm_score,

SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Classify this customer into one of:
Champions, Potential Loyalists, New Customers, At Risk, Cant Lose Them.

Rules:
- Champions: high R, high F, high M
- Potential Loyalists: high R, medium F, good M
- New Customers: high R, low F
- At Risk: low R, high F, high M
- Cant Lose Them: very low R, high F

Customer data:
R=' || r_score || ', F=' || f_score || ', M=' || m_score

|| '. Return ONLY segment name.'
) AS rfm_segment

FROM rfm_final;



-- ALTER TABLE rfm_segments
-- ADD COLUMN extracted_value STRING;


-- UPDATE rfm_segments
-- SET extracted_value = REGEXP_SUBSTR(
--     rfm_segment,
--     '"([^"]+)"',
--     1, 1, 'e', 1
-- );

select distinct extracted_value from rfm_segments;
Potential Loyalist,New Customer, Champion,At Risk



--kpis of seller
SELECT seller_id, seller_name, seller_tier, seller_category, avg_customer_rating, total_orders, total_revenue, seller_score
FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS
ORDER BY avg_customer_rating DESC
LIMIT 10; --ratings

SELECT seller_id, seller_name, seller_tier, seller_category, total_orders, total_revenue, repeat_customer_pct, seller_score
FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS
ORDER BY total_orders DESC
LIMIT 10; --orders

SELECT 
  seller_tier,
  COUNT(*) AS seller_count,
  ROUND(AVG(total_revenue), 2) AS avg_revenue,
  ROUND(AVG(total_orders), 0) AS avg_orders,
  ROUND(AVG(avg_customer_rating), 2) AS avg_rating,
  ROUND(AVG(seller_score), 2) AS avg_seller_score,
  SUM(CASE WHEN is_top_seller THEN 1 ELSE 0 END) AS top_sellers_count
FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS
GROUP BY seller_tier
ORDER BY avg_revenue DESC; --overall


------- seller-order-customer fact table-------- in sheet


--- cotex code for full data----

CREATE OR REPLACE TABLE seller_cortex_final AS
SELECT
g.*,

/* ---------------- CONTEXT STRING ---------------- */
'Seller Summary: Revenue=' || total_order_value ||
', Orders=' || total_orders_actual ||
', ReturnRate=' || actual_return_rate_pct ||
', COD=' || cod_pct ||
', Growth=' || growth_rate_pct ||
', ROI=' || marketing_roi ||
', Category=' || most_sold_category ||
', Rating=' || avg_customer_rating_actual
AS seller_context,

/* ---------------- 1. SUMMARIZATION ---------------- */
SNOWFLAKE.CORTEX.SUMMARIZE(
'Seller Summary: Revenue=' || total_order_value ||
', Orders=' || total_orders_actual ||
', ReturnRate=' || actual_return_rate_pct ||
', Growth=' || growth_rate_pct
) AS concise_summary,

/* ---------------- 2. SENTIMENT ---------------- */
SNOWFLAKE.CORTEX.SENTIMENT(
'Performance=' || performance_segment ||
', Growth=' || growth_segment ||
', ROI=' || marketing_roi
) AS performance_sentiment_score,

/* ---------------- 3. CLASSIFICATION ---------------- */
SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
'Seller Performance: ' || performance_segment || ', Risk: ' || risk_segment,
ARRAY_CONSTRUCT('Top Performer','Good Performer','Average','Risky Seller')
) AS seller_classification,

/* ---------------- 4. KEY INSIGHT EXTRACTION ---------------- */
SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
'Seller has return rate ' || actual_return_rate_pct ||
' and fraud rate ' || fraud_rate_pct,
'What is the main risk?'
) AS extracted_risk,

SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
'Seller category ' || most_sold_category ||
' with revenue ' || total_order_value,
'What is the main strength?'
) AS extracted_strength,

/* ---------------- 5. TARGETED AI ACTION ---------------- */
SNOWFLAKE.CORTEX.COMPLETE(
'snowflake-arctic',
'Give one precise business action: ' ||
'returns=' || actual_return_rate_pct ||
', delivery=' || avg_delivery_days_actual ||
', ROI=' || marketing_roi ||
', growth=' || growth_rate_pct
) AS recommended_action,

/* ---------------- 6. TRANSLATION ---------------- */
SNOWFLAKE.CORTEX.TRANSLATE(
SNOWFLAKE.CORTEX.SUMMARIZE(
'Seller performance with revenue ' || total_order_value ||
' and growth ' || growth_rate_pct
),
'en',
'hi'
) AS summary_hindi,

/* ---------------- 7. EMBEDDINGS ---------------- */
SNOWFLAKE.CORTEX.EMBED_TEXT_1024(
'snowflake-arctic-embed-l-v2.0',
COALESCE(most_sold_category,'') || ' ' ||
COALESCE(performance_segment,'') || ' ' ||
COALESCE(growth_segment,'')
) AS seller_embedding,

/* ---------------- 8. FINAL BUSINESS DECISION ---------------- */
CASE
    WHEN risk_segment = 'High Risk' THEN 'RESTRICT_SELLER'
    WHEN actual_return_rate_pct > 25 THEN 'FIX_PRODUCT_QUALITY'
    WHEN late_delivery_pct > 20 THEN 'CHANGE_LOGISTICS'
    WHEN marketing_roi < 0 THEN 'STOP_MARKETING'
    WHEN growth_segment = 'High Growth' THEN 'BOOST_SELLER'
    WHEN performance_segment = 'Top Performer' THEN 'PRIORITIZE_SELLER'
    ELSE 'OPTIMIZE'
END AS final_action,

CURRENT_TIMESTAMP() AS created_at_seller

FROM SNOWFLAKE_LEARNING_DB.PUBLIC.gold_seller_360 g; --used full table




select * from seller_cortex_final;

--all tables
SHOW TABLES IN SNOWFLAKE_LEARNING_DB.PUBLIC;


-- ============================================================
-- FORECAST TABLE 1: CUSTOMER OUTLOOK
-- ============================================================
-- Predicts: churn, tier migration, CLV growth, RFM drift,
-- upsell potential, revenue at risk, persona stability,
-- marketing responsiveness, and recommended interventions
-- ============================================================

CREATE OR REPLACE TRANSIENT TABLE SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK AS
WITH customer_data AS (
    SELECT
        c.CUSTOMER_ID,
        c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMER_NAME,
        c.CITY,
        c.STATE,
        c.LOYALTY_TIER,
        c.TOTAL_ORDERS,
        c.TOTAL_ORDER_VALUE,
        c.AVG_ORDER_VALUE,
        c.MAX_ORDER_VALUE,
        c.PROFILE_CLV AS CUSTOMER_LIFETIME_VALUE,
        c.CHURN_RISK_SCORE,
        c.PROFILE_ENGAGEMENT_SCORE AS ENGAGEMENT_SCORE,
        c.DISCOUNT_SENSITIVITY_SCORE,
        c.PRICE_SENSITIVITY_SCORE,
        c.RETURN_BEHAVIOR_SCORE,
        c.FRAUD_RISK_SCORE,
        c.LAST_ORDER_DATE,
        c.FIRST_ORDER_DATE,
        c.ORDER_SPAN_DAYS,
        c.TOTAL_RETURNS,
        c.RETURN_RATE_PCT,
        c.UNIQUE_CATEGORIES_PURCHASED,
        c.UNIQUE_BRANDS_PURCHASED,
        c.AVG_ENGAGEMENT_SCORE AS MARKETING_ENGAGEMENT,
        c.AVG_PROPENSITY_SCORE,
        c.OPEN_RATE_PCT,
        c.CLICK_RATE_PCT,
        c.CONVERSION_RATE_PCT,
        c.AVG_FATIGUE_SCORE,
        c.MARKETING_ROI,
        c.PERSONA,
        r.R_SCORE,
        r.F_SCORE,
        r.M_SCORE,
        r.RFM_SCORE,
        r.EXTRACTED_VALUE AS RFM_SEGMENT,
        DATEDIFF(DAY, c.LAST_ORDER_DATE, CURRENT_DATE()) AS DAYS_SINCE_LAST_ORDER,
        CASE WHEN c.ORDER_SPAN_DAYS > 0
            THEN c.TOTAL_ORDERS * 30.0 / c.ORDER_SPAN_DAYS
            ELSE 0
        END AS MONTHLY_ORDER_RATE
    FROM SNOWFLAKE_LEARNING_DB.PUBLIC.CUSTOMER_AI_360 c
    LEFT JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.RFM_SEGMENTS r
        ON c.CUSTOMER_ID = r.CUSTOMER_ID
)
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CITY,
    STATE,
    PERSONA,

    -- 1. LOYALTY TIER MIGRATION
    LOYALTY_TIER AS CURRENT_LOYALTY_TIER,
    CASE
        WHEN LOYALTY_TIER = 'bronze' AND CUSTOMER_LIFETIME_VALUE > 100000 AND ENGAGEMENT_SCORE > 0.6 THEN 'silver'
        WHEN LOYALTY_TIER = 'silver' AND CUSTOMER_LIFETIME_VALUE > 200000 AND ENGAGEMENT_SCORE > 0.7 THEN 'gold'
        WHEN LOYALTY_TIER = 'gold' AND CUSTOMER_LIFETIME_VALUE > 350000 AND ENGAGEMENT_SCORE > 0.8 THEN 'platinum'
        WHEN LOYALTY_TIER = 'platinum' AND CHURN_RISK_SCORE > 0.7 AND ENGAGEMENT_SCORE < 0.3 THEN 'gold'
        WHEN LOYALTY_TIER = 'gold' AND CHURN_RISK_SCORE > 0.7 AND ENGAGEMENT_SCORE < 0.3 THEN 'silver'
        ELSE LOYALTY_TIER
    END AS PREDICTED_LOYALTY_TIER,

    -- 2. RFM SEGMENT MIGRATION
    RFM_SEGMENT AS CURRENT_RFM_SEGMENT,
    CASE
        WHEN RFM_SEGMENT = 'Champion' AND DAYS_SINCE_LAST_ORDER > 60 THEN 'At Risk'
        WHEN RFM_SEGMENT = 'Champion' AND DAYS_SINCE_LAST_ORDER > 30 THEN 'Potential Loyalist'
        WHEN RFM_SEGMENT = 'Potential Loyalist' AND MONTHLY_ORDER_RATE >= 3 AND M_SCORE >= 4 THEN 'Champion'
        WHEN RFM_SEGMENT = 'Potential Loyalist' AND DAYS_SINCE_LAST_ORDER > 90 THEN 'At Risk'
        WHEN RFM_SEGMENT = 'New Customer' AND F_SCORE >= 3 AND M_SCORE >= 3 THEN 'Potential Loyalist'
        WHEN RFM_SEGMENT = 'New Customer' AND DAYS_SINCE_LAST_ORDER > 60 THEN 'At Risk'
        WHEN RFM_SEGMENT = 'At Risk' AND DAYS_SINCE_LAST_ORDER > 120 THEN 'At Risk'
        ELSE RFM_SEGMENT
    END AS PREDICTED_RFM_SEGMENT,

    -- 3. CHURN PREDICTION
    ROUND(CHURN_RISK_SCORE, 3) AS CURRENT_CHURN_RISK,
    CASE
        WHEN CHURN_RISK_SCORE > 0.7 AND DAYS_SINCE_LAST_ORDER > 60 AND ENGAGEMENT_SCORE < 0.3 THEN 'VERY HIGH'
        WHEN CHURN_RISK_SCORE > 0.7 OR (DAYS_SINCE_LAST_ORDER > 45 AND ENGAGEMENT_SCORE < 0.4) THEN 'HIGH'
        WHEN CHURN_RISK_SCORE > 0.4 OR DAYS_SINCE_LAST_ORDER > 30 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS CHURN_PREDICTION_30D,
    ROUND(
        LEAST(1.0,
            CHURN_RISK_SCORE * 0.4
            + LEAST(1.0, DAYS_SINCE_LAST_ORDER / 120.0) * 0.3
            + (1 - ENGAGEMENT_SCORE) * 0.2
            + (1 - LEAST(1.0, MONTHLY_ORDER_RATE / 5.0)) * 0.1
        ), 3
    ) AS CHURN_PROBABILITY_SCORE,

    -- 4. CLV PROJECTION
    ROUND(CUSTOMER_LIFETIME_VALUE, 2) AS CURRENT_CLV,
    ROUND(CUSTOMER_LIFETIME_VALUE + (MONTHLY_ORDER_RATE * AVG_ORDER_VALUE * 3), 2) AS PROJECTED_CLV_90D,
    ROUND(
        CASE WHEN CUSTOMER_LIFETIME_VALUE > 0
            THEN ((MONTHLY_ORDER_RATE * AVG_ORDER_VALUE * 3) / CUSTOMER_LIFETIME_VALUE) * 100
            ELSE 0
        END, 2
    ) AS CLV_GROWTH_PCT_90D,

    -- 5. ORDER FREQUENCY FORECAST
    ROUND(MONTHLY_ORDER_RATE, 2) AS CURRENT_MONTHLY_ORDER_RATE,
    ROUND(
        CASE
            WHEN DAYS_SINCE_LAST_ORDER > 90 THEN MONTHLY_ORDER_RATE * 0.3
            WHEN DAYS_SINCE_LAST_ORDER > 60 THEN MONTHLY_ORDER_RATE * 0.5
            WHEN DAYS_SINCE_LAST_ORDER > 30 THEN MONTHLY_ORDER_RATE * 0.8
            ELSE MONTHLY_ORDER_RATE
        END, 2
    ) AS PREDICTED_ORDERS_NEXT_30D,

    -- 6. UPSELL POTENTIAL
    ROUND(
        (CASE WHEN MAX_ORDER_VALUE > 0 AND AVG_ORDER_VALUE > 0
            THEN (1 - AVG_ORDER_VALUE / MAX_ORDER_VALUE) * 0.4
            ELSE 0 END)
        + (LEAST(1.0, UNIQUE_CATEGORIES_PURCHASED / 10.0) * 0.3)
        + (LEAST(1.0, UNIQUE_BRANDS_PURCHASED / 10.0) * 0.3)
    , 3) AS UPSELL_POTENTIAL_SCORE,

    -- 7. REVENUE AT RISK
    ROUND(
        CUSTOMER_LIFETIME_VALUE * LEAST(1.0,
            CHURN_RISK_SCORE * 0.4
            + LEAST(1.0, DAYS_SINCE_LAST_ORDER / 120.0) * 0.3
            + (1 - ENGAGEMENT_SCORE) * 0.2
            + (1 - LEAST(1.0, MONTHLY_ORDER_RATE / 5.0)) * 0.1
        ), 2
    ) AS REVENUE_AT_RISK,

    -- 8. RETENTION PRIORITY
    CASE
        WHEN CUSTOMER_LIFETIME_VALUE * CHURN_RISK_SCORE > 100000 THEN 'CRITICAL'
        WHEN CHURN_RISK_SCORE > 0.7 AND CUSTOMER_LIFETIME_VALUE > 50000 THEN 'HIGH'
        WHEN CHURN_RISK_SCORE > 0.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS RETENTION_PRIORITY,

    -- 9. PERSONA STABILITY
    CASE
        WHEN ENGAGEMENT_SCORE > 0.7 AND DISCOUNT_SENSITIVITY_SCORE < 0.3 THEN 'STABLE'
        WHEN ENGAGEMENT_SCORE > 0.5 AND DISCOUNT_SENSITIVITY_SCORE < 0.5 THEN 'LIKELY STABLE'
        WHEN CHURN_RISK_SCORE > 0.6 OR ENGAGEMENT_SCORE < 0.3 THEN 'LIKELY SHIFTING'
        ELSE 'MONITOR'
    END AS PERSONA_STABILITY,

    -- 10. MARKETING RESPONSIVENESS
    ROUND(
        COALESCE(MARKETING_ENGAGEMENT, 0) * 0.3
        + COALESCE(OPEN_RATE_PCT, 0) / 100.0 * 0.2
        + COALESCE(CLICK_RATE_PCT, 0) / 100.0 * 0.2
        + COALESCE(CONVERSION_RATE_PCT, 0) / 100.0 * 0.2
        + (1 - COALESCE(AVG_FATIGUE_SCORE, 0)) * 0.1
    , 3) AS MARKETING_RESPONSIVENESS_SCORE,

    -- RECOMMENDED INTERVENTION (AI-driven action)
    CASE
        WHEN CUSTOMER_LIFETIME_VALUE * CHURN_RISK_SCORE > 100000 THEN 'IMMEDIATE_WINBACK_CAMPAIGN'
        WHEN CHURN_RISK_SCORE > 0.7 AND DAYS_SINCE_LAST_ORDER > 60 AND ENGAGEMENT_SCORE < 0.3 THEN 'PERSONALIZED_RETENTION_OFFER'
        WHEN CHURN_RISK_SCORE > 0.7 THEN 'LOYALTY_BONUS_ACTIVATION'
        WHEN (CASE WHEN MAX_ORDER_VALUE > 0 AND AVG_ORDER_VALUE > 0
                THEN (1 - AVG_ORDER_VALUE / MAX_ORDER_VALUE) * 0.4 ELSE 0 END
              + LEAST(1.0, UNIQUE_CATEGORIES_PURCHASED / 10.0) * 0.3
              + LEAST(1.0, UNIQUE_BRANDS_PURCHASED / 10.0) * 0.3) > 0.6 THEN 'UPSELL_CROSS_SELL_CAMPAIGN'
        WHEN CHURN_RISK_SCORE > 0.6 OR ENGAGEMENT_SCORE < 0.3 THEN 'ENGAGEMENT_REACTIVATION'
        ELSE 'STANDARD_ENGAGEMENT'
    END AS RECOMMENDED_INTERVENTION,

    CURRENT_TIMESTAMP() AS FORECAST_GENERATED_AT

FROM customer_data;


SELECT * FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK LIMIT 10;


-- ============================================================
-- FORECAST TABLE 2: SELLER OUTLOOK
-- ============================================================
-- Predicts: tier migration, revenue projection, risk escalation,
-- customer base growth, operational health, profit trajectory,
-- sustainability, revenue at risk, and investment recommendation
-- ============================================================

CREATE OR REPLACE TRANSIENT TABLE SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK AS
WITH seller_data AS (

    SELECT
        s.SELLER_ID,
        s.SELLER_NAME,
        s.SELLER_TYPE,
        s.SELLER_CATEGORY,
        s.SELLER_TIER,
        s.SELLER_CITY,
        s.YEARS_ON_PLATFORM,
        s.TOTAL_ORDERS_ACTUAL,
        s.UNIQUE_CUSTOMERS,
        s.TOTAL_ORDER_VALUE,
        s.AVG_ORDER_VALUE,
        s.PROFILE_MONTHLY_SALES AS MONTHLY_SALES,
        s.GROWTH_RATE_PCT,
        s.TOTAL_RETURNS,
        s.ACTUAL_RETURN_RATE_PCT,
        s.ACTUAL_CANCEL_RATE_PCT,
        s.RTO_RATE_PCT,
        s.FRAUD_FLAG_RATE,
        s.FRAUD_RATE_PCT,
        s.AVG_DELIVERY_DAYS_ACTUAL,
        s.LATE_DELIVERY_PCT,
        s.TOTAL_PROFIT,
        s.PROFIT_MARGIN_PCT,
        s.AVG_MARGIN_PCT,
        s.AVG_DISCOUNT_PCT,
        s.REPEAT_CUSTOMER_PCT,
        s.AVG_CUSTOMER_CLV,
        s.AVG_CUSTOMER_CHURN_RISK,
        s.MARKETING_SPEND,
        s.MARKETING_ROI,
        s.SELLER_SCORE,
        s.SELLER_RISK_SCORE,
        s.SELLER_GROWTH_SCORE,
        s.PERFORMANCE_SEGMENT,
        s.GROWTH_SEGMENT,
        s.RISK_SEGMENT,
        s.IS_TOP_SELLER,
        s.IS_HIGH_RETURN_SELLER,
        s.IS_HIGH_RISK_SELLER
    FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLER_CORTEX_FINAL s
)
SELECT
    SELLER_ID,
    SELLER_NAME,
    SELLER_TYPE,
    SELLER_CATEGORY,
    SELLER_CITY,
    YEARS_ON_PLATFORM,

    -- 1. TIER MIGRATION
    SELLER_TIER AS CURRENT_SELLER_TIER,
    CASE
        WHEN SELLER_TIER = 'bronze' AND SELLER_SCORE >= 60 AND SELLER_GROWTH_SCORE >= 50 THEN 'silver'
        WHEN SELLER_TIER = 'silver' AND SELLER_SCORE >= 75 AND SELLER_GROWTH_SCORE >= 65 THEN 'gold'
        WHEN SELLER_TIER = 'gold' AND SELLER_SCORE >= 90 AND SELLER_GROWTH_SCORE >= 80 THEN 'platinum'
        WHEN SELLER_TIER = 'platinum' AND (SELLER_RISK_SCORE >= 60 OR GROWTH_RATE_PCT < -10) THEN 'gold'
        WHEN SELLER_TIER = 'gold' AND (SELLER_RISK_SCORE >= 70 OR GROWTH_RATE_PCT < -15) THEN 'silver'
        WHEN SELLER_TIER = 'silver' AND (SELLER_RISK_SCORE >= 80 OR GROWTH_RATE_PCT < -20) THEN 'bronze'
        ELSE SELLER_TIER
    END AS PREDICTED_SELLER_TIER,

    -- 2. REVENUE PROJECTION (90-day)
    ROUND(TOTAL_ORDER_VALUE, 2) AS CURRENT_REVENUE,
    ROUND(
        CASE WHEN MONTHLY_SALES > 0
            THEN MONTHLY_SALES * 3 * (1 + GROWTH_RATE_PCT / 100.0)
            ELSE TOTAL_ORDER_VALUE * 0.25 * (1 + GROWTH_RATE_PCT / 100.0)
        END, 2
    ) AS PROJECTED_REVENUE_90D,
    ROUND(GROWTH_RATE_PCT, 2) AS CURRENT_GROWTH_RATE,
    CASE
        WHEN GROWTH_RATE_PCT >= 30 THEN 'ACCELERATING'
        WHEN GROWTH_RATE_PCT >= 10 THEN 'GROWING'
        WHEN GROWTH_RATE_PCT >= 0 THEN 'STABLE'
        WHEN GROWTH_RATE_PCT >= -10 THEN 'SLOWING'
        ELSE 'DECLINING'
    END AS REVENUE_GROWTH_TREND,

    -- 3. RISK ESCALATION
    RISK_SEGMENT AS CURRENT_RISK_SEGMENT,
    ROUND(
        LEAST(1.0,
            (COALESCE(ACTUAL_RETURN_RATE_PCT, 0) / 50.0) * 0.25
            + (COALESCE(ACTUAL_CANCEL_RATE_PCT, 0) / 30.0) * 0.20
            + (COALESCE(FRAUD_RATE_PCT, 0) / 10.0) * 0.25
            + (COALESCE(SELLER_RISK_SCORE, 0) / 100.0) * 0.20
            + (CASE WHEN GROWTH_RATE_PCT < 0 THEN ABS(GROWTH_RATE_PCT) / 20.0 ELSE 0 END) * 0.10
        ), 3
    ) AS RISK_ESCALATION_PROBABILITY,
    CASE
        WHEN SELLER_RISK_SCORE >= 70 AND ACTUAL_RETURN_RATE_PCT > 20 THEN 'CRITICAL'
        WHEN SELLER_RISK_SCORE >= 60 OR ACTUAL_RETURN_RATE_PCT > 15 OR FRAUD_RATE_PCT > 5 THEN 'HIGH'
        WHEN SELLER_RISK_SCORE >= 40 OR ACTUAL_RETURN_RATE_PCT > 10 THEN 'ELEVATED'
        ELSE 'STABLE'
    END AS RISK_FORECAST,

    -- 4. CUSTOMER BASE GROWTH
    UNIQUE_CUSTOMERS AS CURRENT_UNIQUE_CUSTOMERS,
    ROUND(REPEAT_CUSTOMER_PCT, 2) AS REPEAT_CUSTOMER_PCT,
    CASE
        WHEN GROWTH_RATE_PCT > 20 AND REPEAT_CUSTOMER_PCT > 40 THEN 'STRONG GROWTH'
        WHEN GROWTH_RATE_PCT > 10 AND REPEAT_CUSTOMER_PCT > 30 THEN 'MODERATE GROWTH'
        WHEN GROWTH_RATE_PCT > 0 THEN 'STABLE'
        WHEN REPEAT_CUSTOMER_PCT > 50 THEN 'RETAINED BUT SHRINKING'
        ELSE 'DECLINING'
    END AS CUSTOMER_BASE_TREND,

    -- 5. OPERATIONAL HEALTH
    ROUND(AVG_DELIVERY_DAYS_ACTUAL, 1) AS CURRENT_AVG_DELIVERY_DAYS,
    ROUND(LATE_DELIVERY_PCT, 2) AS CURRENT_LATE_DELIVERY_PCT,
    ROUND(
        GREATEST(0, LEAST(100,
            100
            - (GREATEST(0, AVG_DELIVERY_DAYS_ACTUAL - 3) * 8)
            - (LATE_DELIVERY_PCT * 1.5)
            - (ACTUAL_RETURN_RATE_PCT * 1.0)
            - (ACTUAL_CANCEL_RATE_PCT * 1.2)
        )), 2
    ) AS OPERATIONAL_HEALTH_SCORE,
    CASE
        WHEN LATE_DELIVERY_PCT > 30 OR AVG_DELIVERY_DAYS_ACTUAL > 8 THEN 'DETERIORATING'
        WHEN LATE_DELIVERY_PCT > 15 OR AVG_DELIVERY_DAYS_ACTUAL > 6 THEN 'AT RISK'
        WHEN LATE_DELIVERY_PCT > 5 THEN 'ACCEPTABLE'
        ELSE 'HEALTHY'
    END AS OPERATIONAL_HEALTH_FORECAST,

    -- 6. PROFIT MARGIN TRAJECTORY
    ROUND(PROFIT_MARGIN_PCT, 2) AS CURRENT_PROFIT_MARGIN_PCT,
    CASE
        WHEN PROFIT_MARGIN_PCT > 15 AND AVG_DISCOUNT_PCT < 10 THEN 'STRONG & SUSTAINABLE'
        WHEN PROFIT_MARGIN_PCT > 10 THEN 'HEALTHY'
        WHEN PROFIT_MARGIN_PCT > 5 AND AVG_DISCOUNT_PCT > 20 THEN 'DISCOUNT DEPENDENT'
        WHEN PROFIT_MARGIN_PCT > 0 THEN 'THIN MARGINS'
        ELSE 'LOSS MAKING'
    END AS PROFIT_TRAJECTORY,

    -- 7. PERFORMANCE TIER SHIFT
    PERFORMANCE_SEGMENT AS CURRENT_PERFORMANCE,
    CASE
        WHEN PERFORMANCE_SEGMENT = 'Top Performer' AND GROWTH_RATE_PCT < -10 THEN 'Good Performer'
        WHEN PERFORMANCE_SEGMENT = 'Good Performer' AND SELLER_SCORE >= 80 AND GROWTH_RATE_PCT > 15 THEN 'Top Performer'
        WHEN PERFORMANCE_SEGMENT = 'Good Performer' AND GROWTH_RATE_PCT < -15 THEN 'Average Performer'
        WHEN PERFORMANCE_SEGMENT = 'Average Performer' AND SELLER_SCORE >= 65 AND GROWTH_RATE_PCT > 20 THEN 'Good Performer'
        WHEN PERFORMANCE_SEGMENT = 'Average Performer' AND SELLER_RISK_SCORE >= 70 THEN 'Needs Improvement'
        ELSE PERFORMANCE_SEGMENT
    END AS PREDICTED_PERFORMANCE,

    -- 8. PLATFORM SUSTAINABILITY SCORE
    ROUND(
        (SELLER_SCORE / 100.0) * 0.25
        + (SELLER_GROWTH_SCORE / 100.0) * 0.20
        + ((100 - SELLER_RISK_SCORE) / 100.0) * 0.20
        + (COALESCE(REPEAT_CUSTOMER_PCT, 0) / 100.0) * 0.15
        + (LEAST(100, COALESCE(PROFIT_MARGIN_PCT, 0)) / 100.0) * 0.10
        + (COALESCE(MARKETING_ROI, 0) / GREATEST(1, ABS(COALESCE(MARKETING_ROI, 1)))) * 0.10
    , 3) AS SUSTAINABILITY_SCORE,

    -- 9. REVENUE AT RISK
    ROUND(
        TOTAL_ORDER_VALUE * LEAST(1.0,
            (COALESCE(ACTUAL_RETURN_RATE_PCT, 0) / 50.0) * 0.25
            + (COALESCE(ACTUAL_CANCEL_RATE_PCT, 0) / 30.0) * 0.20
            + (COALESCE(FRAUD_RATE_PCT, 0) / 10.0) * 0.25
            + (COALESCE(SELLER_RISK_SCORE, 0) / 100.0) * 0.20
            + (CASE WHEN GROWTH_RATE_PCT < 0 THEN ABS(GROWTH_RATE_PCT) / 20.0 ELSE 0 END) * 0.10
        ), 2
    ) AS REVENUE_AT_RISK,

    -- 10. INVESTMENT RECOMMENDATION
    CASE
        WHEN IS_HIGH_RISK_SELLER AND GROWTH_RATE_PCT < 0 THEN 'DIVEST'
        WHEN SELLER_RISK_SCORE >= 70 THEN 'RESTRICT'
        WHEN SELLER_SCORE >= 80 AND GROWTH_RATE_PCT >= 20 THEN 'INVEST HEAVILY'
        WHEN SELLER_SCORE >= 60 AND GROWTH_RATE_PCT >= 10 THEN 'INVEST MODERATELY'
        WHEN SELLER_SCORE >= 60 AND GROWTH_RATE_PCT >= 0 THEN 'MAINTAIN'
        WHEN GROWTH_RATE_PCT < -10 THEN 'REDUCE EXPOSURE'
        ELSE 'MONITOR'
    END AS INVESTMENT_RECOMMENDATION,

    -- RECOMMENDED ACTION SUMMARY
    CASE
        WHEN IS_HIGH_RISK_SELLER AND ACTUAL_RETURN_RATE_PCT > 20 THEN 'IMMEDIATE_QUALITY_AUDIT'
        WHEN SELLER_RISK_SCORE >= 70 THEN 'RISK_MITIGATION_PLAN'
        WHEN LATE_DELIVERY_PCT > 30 THEN 'LOGISTICS_OVERHAUL'
        WHEN GROWTH_RATE_PCT < -15 THEN 'GROWTH_RECOVERY_PROGRAM'
        WHEN MARKETING_ROI < 0 THEN 'MARKETING_OPTIMIZATION'
        WHEN SELLER_SCORE >= 80 AND GROWTH_RATE_PCT >= 20 THEN 'SCALE_UP_SUPPORT'
        WHEN PROFIT_MARGIN_PCT < 5 AND AVG_DISCOUNT_PCT > 20 THEN 'PRICING_STRATEGY_REVIEW'
        ELSE 'STANDARD_MONITORING'
    END AS RECOMMENDED_ACTION,

    CURRENT_TIMESTAMP() AS FORECAST_GENERATED_AT

FROM seller_data;


SELECT * FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK LIMIT 10;


