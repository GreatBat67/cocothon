CREATE OR REPLACE NOTIFICATION INTEGRATION ecommerce_email_alerts
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('ronak.b.laddha@kipi.ai');

CREATE OR REPLACE PROCEDURE SNOWFLAKE_LEARNING_DB.PUBLIC.SP_CHECK_AND_SEND_ALERTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    alert_body STRING DEFAULT '';
    alert_count INT DEFAULT 0;
    v_count INT;
    v_revenue FLOAT;
    v_count2 INT;
    table_rows STRING;
    email_subject STRING;
    result_msg STRING;
BEGIN
    alert_body := '<html><body style="font-family:Arial,sans-serif;color:#333;">';
    alert_body := alert_body || '<h1 style="color:#1a73e8;">E-Commerce Intelligence Platform - Alert Report</h1>';
    alert_body := alert_body || '<p style="color:#666;">Generated: ' || CURRENT_TIMESTAMP()::STRING || '</p><hr/>';

    -- 1. SELLER FRAUD DETECTION
    SELECT COUNT(*) INTO v_count FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS WHERE fraud_flag_rate > 5;
    IF (v_count > 0) THEN
        alert_count := alert_count + 1;
        SELECT LISTAGG('<tr><td>' || seller_name || '</td><td>' || seller_category || '</td><td style="color:red;font-weight:bold;">' || fraud_flag_rate::STRING || '%</td><td>' || rto_rate_pct::STRING || '%</td><td>' || cancellation_rate_pct::STRING || '%</td><td>$' || ROUND(total_revenue,0)::STRING || '</td></tr>', '') INTO table_rows
        FROM (SELECT * FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS WHERE fraud_flag_rate > 5 ORDER BY fraud_flag_rate DESC LIMIT 10);
        alert_body := alert_body || '<h2 style="color:#d32f2f;">FRAUD ALERT: ' || v_count::STRING || ' Sellers with High Fraud Rate (>5%)</h2>';
        alert_body := alert_body || '<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%;"><tr style="background:#d32f2f;color:white;"><th>Seller</th><th>Category</th><th>Fraud Rate</th><th>RTO Rate</th><th>Cancel Rate</th><th>Revenue</th></tr>' || table_rows || '</table><br/>';
    END IF;

    -- 2. CUSTOMER CHURN RISK
    SELECT COUNT(*), ROUND(COALESCE(SUM(revenue_at_risk),0),0) INTO v_count, v_revenue
    FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK WHERE churn_prediction_30d IN ('VERY HIGH','HIGH');
    IF (v_count > 0) THEN
        alert_count := alert_count + 1;
        SELECT LISTAGG('<tr><td>' || customer_name || '</td><td>' || COALESCE(persona,'N/A') || '</td><td style="color:red;">' || churn_prediction_30d || '</td><td>$' || ROUND(revenue_at_risk,0)::STRING || '</td><td>' || COALESCE(recommended_intervention,'N/A') || '</td></tr>', '') INTO table_rows
        FROM (SELECT * FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK WHERE churn_prediction_30d IN ('VERY HIGH','HIGH') ORDER BY revenue_at_risk DESC LIMIT 10);
        alert_body := alert_body || '<h2 style="color:#e65100;">CHURN ALERT: ' || v_count::STRING || ' Customers at Risk | $' || v_revenue::STRING || ' Revenue at Risk</h2>';
        alert_body := alert_body || '<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%;"><tr style="background:#e65100;color:white;"><th>Customer</th><th>Persona</th><th>Churn Level</th><th>Rev at Risk</th><th>Intervention</th></tr>' || table_rows || '</table><br/>';
    END IF;

    -- 3. CRITICAL RETENTION
    SELECT COUNT(*), ROUND(COALESCE(SUM(revenue_at_risk),0),0) INTO v_count, v_revenue
    FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK WHERE retention_priority = 'CRITICAL';
    IF (v_count > 0) THEN
        alert_count := alert_count + 1;
        alert_body := alert_body || '<h2 style="color:#b71c1c;">CRITICAL RETENTION: ' || v_count::STRING || ' Customers Need Immediate Winback ($' || v_revenue::STRING || ' at risk)</h2>';
    END IF;

    -- 4. SELLER RISK ESCALATION
    SELECT COUNT(*), ROUND(COALESCE(SUM(revenue_at_risk),0),0) INTO v_count, v_revenue
    FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK WHERE risk_forecast IN ('CRITICAL','HIGH');
    IF (v_count > 0) THEN
        alert_count := alert_count + 1;
        SELECT LISTAGG('<tr><td>' || seller_name || '</td><td>' || seller_category || '</td><td style="color:red;">' || risk_forecast || '</td><td>' || ROUND(operational_health_score,1)::STRING || '</td><td>$' || ROUND(revenue_at_risk,0)::STRING || '</td><td>' || recommended_action || '</td></tr>', '') INTO table_rows
        FROM (SELECT * FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK WHERE risk_forecast IN ('CRITICAL','HIGH') ORDER BY revenue_at_risk DESC LIMIT 10);
        alert_body := alert_body || '<h2 style="color:#c62828;">SELLER RISK: ' || v_count::STRING || ' Sellers at CRITICAL/HIGH ($' || v_revenue::STRING || ' at risk)</h2>';
        alert_body := alert_body || '<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%;"><tr style="background:#c62828;color:white;"><th>Seller</th><th>Category</th><th>Risk</th><th>Ops Health</th><th>Rev at Risk</th><th>Action</th></tr>' || table_rows || '</table><br/>';
    END IF;

    -- 5. TIER DOWNGRADES
    SELECT COUNT(*) INTO v_count FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK
    WHERE (current_loyalty_tier='platinum' AND predicted_loyalty_tier IN ('gold','silver','bronze')) OR (current_loyalty_tier='gold' AND predicted_loyalty_tier IN ('silver','bronze'));
    SELECT COUNT(*) INTO v_count2 FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK
    WHERE (current_seller_tier='platinum' AND predicted_seller_tier IN ('gold','silver','bronze')) OR (current_seller_tier='gold' AND predicted_seller_tier IN ('silver','bronze'));
    IF (v_count > 0 OR v_count2 > 0) THEN
        alert_count := alert_count + 1;
        alert_body := alert_body || '<h2 style="color:#f57c00;">TIER DOWNGRADE: ' || v_count::STRING || ' Customers + ' || v_count2::STRING || ' Sellers dropping tier</h2>';
    END IF;

    -- 6. OPS HEALTH
    SELECT COUNT(*) INTO v_count FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK WHERE operational_health_forecast = 'AT RISK';
    IF (v_count > 0) THEN
        alert_count := alert_count + 1;
        alert_body := alert_body || '<h2 style="color:#6a1b9a;">OPS HEALTH: ' || v_count::STRING || ' Sellers AT RISK</h2>';
    END IF;

    -- 7. HIGH RTO
    SELECT COUNT(*) INTO v_count FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS WHERE rto_rate_pct > 10;
    IF (v_count > 0) THEN
        alert_count := alert_count + 1;
        alert_body := alert_body || '<h2 style="color:#4a148c;">LOGISTICS: ' || v_count::STRING || ' Sellers with RTO >10%</h2>';
    END IF;

    -- 8. CAMPAIGN RE-TARGETING
    SELECT COUNT(*) INTO v_count FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK
    WHERE persona_stability = 'LIKELY SHIFTING' AND upsell_potential_score > 0.5;
    IF (v_count > 0) THEN
        alert_count := alert_count + 1;
        alert_body := alert_body || '<h2 style="color:#0277bd;">CAMPAIGN OPTIMIZATION: ' || v_count::STRING || ' High-Potential Shifting Personas Need Re-targeting</h2>';
    END IF;

    -- 9. INVENTORY
    SELECT COUNT(*) INTO v_count FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS WHERE stock_out_rate > 15;
    IF (v_count > 0) THEN
        alert_count := alert_count + 1;
        SELECT LISTAGG('<tr><td>' || seller_name || '</td><td>' || seller_category || '</td><td style="color:red;">' || ROUND(stock_out_rate,1)::STRING || '%</td><td>' || ROUND(inventory_turnover_ratio,2)::STRING || '</td><td>' || avg_stock_level::STRING || '</td></tr>', '') INTO table_rows
        FROM (SELECT * FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS WHERE stock_out_rate > 15 ORDER BY stock_out_rate DESC LIMIT 10);
        alert_body := alert_body || '<h2 style="color:#2e7d32;">INVENTORY: ' || v_count::STRING || ' Sellers with Stock-Out >15%</h2>';
        alert_body := alert_body || '<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%;"><tr style="background:#2e7d32;color:white;"><th>Seller</th><th>Category</th><th>Stock-Out</th><th>Turnover</th><th>Avg Stock</th></tr>' || table_rows || '</table><br/>';
    END IF;

    -- 10. COD DEPENDENCY
    SELECT COUNT(*) INTO v_count FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS WHERE cod_users_pct > 60;
    IF (v_count > 0) THEN
        alert_count := alert_count + 1;
        alert_body := alert_body || '<h2 style="color:#795548;">PAYMENT RISK: ' || v_count::STRING || ' Sellers with >60% COD Dependency</h2>';
    END IF;

    -- 11. SUSTAINABILITY
    SELECT COUNT(*) INTO v_count FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK
    WHERE sustainability_score < 0.4 AND current_revenue > 100000;
    IF (v_count > 0) THEN
        alert_count := alert_count + 1;
        alert_body := alert_body || '<h2 style="color:#37474f;">SUSTAINABILITY: ' || v_count::STRING || ' High-Rev Sellers with Low Sustainability</h2>';
    END IF;

    alert_body := alert_body || '<hr/><p style="color:#999;font-size:12px;">Total alerts: ' || alert_count::STRING || ' | Automated by E-Commerce Intelligence Platform</p></body></html>';

    email_subject := 'E-Commerce Alert Report: ' || alert_count::STRING || ' Issues [' || CURRENT_DATE()::STRING || ']';
    result_msg := 'Sent ' || alert_count::STRING || ' alerts to ronak.b.laddha@kipi.ai';

    IF (alert_count > 0) THEN
        CALL SYSTEM$SEND_EMAIL('ecommerce_email_alerts', 'ronak.b.laddha@kipi.ai', :email_subject, :alert_body, 'text/html');
        RETURN :result_msg;
    ELSE
        RETURN 'No alerts triggered. All systems healthy.';
    END IF;
END;
$$

CALL SNOWFLAKE_LEARNING_DB.PUBLIC.SP_CHECK_AND_SEND_ALERTS();


SELECT SELLER_ID, SELLER_NAME,
       BASELINE_RISK_FORECAST, REALTIME_RISK_FORECAST,
       BASELINE_REVENUE, REALTIME_REVENUE,
       REALTIME_ORDER_COUNT, REALTIME_ORDER_VALUE, REALTIME_REVENUE_LIFT_PCT,
       PREDICTION_CHANGED, LAST_REFRESHED_AT
FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_FORECAST_SELLER_OUTLOOK
WHERE PREDICTION_CHANGED = TRUE
ORDER BY REALTIME_ORDER_VALUE DESC
LIMIT 20;

CALL SNOWFLAKE_LEARNING_DB.PUBLIC.SP_CHECK_AND_SEND_ALERTS();

CREATE OR REPLACE TASK SNOWFLAKE_LEARNING_DB.PUBLIC.TASK_DAILY_EMAIL_ALERTS
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 9 * * * Asia/Kolkata'
AS
  CALL SNOWFLAKE_LEARNING_DB.PUBLIC.SP_CHECK_AND_SEND_ALERTS();


ALTER TASK SNOWFLAKE_LEARNING_DB.PUBLIC.TASK_DAILY_EMAIL_ALERTS RESUME;