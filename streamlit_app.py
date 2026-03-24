import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("E-Commerce Data Explorer")

TABLES = [
    "CUSTOMERS",
    "ORDERS", 
    "SELLERS",
    "SELLER_ORDER_CUSTOMER_METRICS",
    "CUSTOMER_SELLER_MAPPING",
    "CUSTOMER_AI_360",
    "GOLD_CUSTOMER_360",
    "GOLD_SELLER_360",
    "RFM_SEGMENTS",
    "FORECAST_CUSTOMER_OUTLOOK",
    "FORECAST_SELLER_OUTLOOK",
]

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    ":material/table_chart: Tables",
    ":material/person: Customers",
    ":material/storefront: Sellers",
    ":material/analytics: Analytics",
    ":material/trending_up: Customer Forecast",
    ":material/inventory: Seller Forecast",
    ":material/chat: AI Chat",
    ":material/mail: Email Alerts",
])

with tab1:
    st.subheader("Browse Tables")
    selected_table = st.selectbox("Select table", TABLES)
    
    if selected_table:
        row_limit = st.slider("Rows to display", 10, 500, 100)
        df = session.table(f"SNOWFLAKE_LEARNING_DB.PUBLIC.{selected_table}").limit(row_limit).to_pandas()
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption(f"Showing {len(df)} rows")

with tab2:
    st.subheader("Customer Insights")
    
    col1, col2, col3 = st.columns(3)
    
    cust_stats = session.sql("""
        SELECT 
            COUNT(*) as total_customers,
            COUNT(DISTINCT loyalty_tier) as tiers,
            ROUND(AVG(customer_lifetime_value), 2) as avg_clv
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.CUSTOMERS
    """).to_pandas()
    
    col1.metric("Total Customers", f"{int(cust_stats['TOTAL_CUSTOMERS'][0]):,}")
    col2.metric("Loyalty Tiers", int(cust_stats['TIERS'][0]))
    col3.metric("Avg CLV", f"${float(cust_stats['AVG_CLV'][0]):,.0f}")
    
    st.subheader("Customers by Loyalty Tier")
    tier_df = session.sql("""
        SELECT loyalty_tier, COUNT(*) as count, 
               ROUND(AVG(customer_lifetime_value), 2) as avg_clv
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.CUSTOMERS
        GROUP BY loyalty_tier ORDER BY count DESC
    """).to_pandas()
    st.bar_chart(tier_df, x="LOYALTY_TIER", y="COUNT")
    
    st.subheader("Top Customers by CLV")
    top_customers = session.sql("""
        SELECT customer_id, first_name || ' ' || last_name as name, 
               city, loyalty_tier, total_orders, 
               ROUND(customer_lifetime_value, 2) as clv
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.CUSTOMERS
        ORDER BY customer_lifetime_value DESC LIMIT 10
    """).to_pandas()
    st.dataframe(top_customers, hide_index=True)

with tab3:
    st.subheader("Seller Insights")
    
    col1, col2, col3 = st.columns(3)
    
    seller_stats = session.sql("""
        SELECT 
            COUNT(*) as total_sellers,
            ROUND(AVG(total_revenue), 0) as avg_revenue,
            ROUND(AVG(avg_customer_rating), 2) as avg_rating
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS
    """).to_pandas()
    
    col1.metric("Total Sellers", int(seller_stats['TOTAL_SELLERS'][0]))
    col2.metric("Avg Revenue", f"${float(seller_stats['AVG_REVENUE'][0]):,.0f}")
    col3.metric("Avg Rating", f"{float(seller_stats['AVG_RATING'][0]):.1f}")
    
    st.subheader("Sellers by Tier")
    tier_seller = session.sql("""
        SELECT seller_tier, COUNT(*) as count, 
               ROUND(AVG(total_revenue), 0) as avg_revenue
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS
        GROUP BY seller_tier ORDER BY avg_revenue DESC
    """).to_pandas()
    st.bar_chart(tier_seller, x="SELLER_TIER", y="AVG_REVENUE")
    
    st.subheader("Top Sellers by Revenue")
    top_sellers = session.sql("""
        SELECT seller_id, seller_name, seller_tier, seller_category,
               total_orders, ROUND(total_revenue, 0) as revenue,
               avg_customer_rating as rating
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS
        ORDER BY total_revenue DESC LIMIT 10
    """).to_pandas()
    st.dataframe(top_sellers, hide_index=True)

with tab4:
    st.subheader("Combined Analytics")

    st.write("**Orders by Seller Tier & Category**")
    combo = session.sql("""
        SELECT seller_tier, category, COUNT(*) as orders,
               ROUND(SUM(order_value), 0) as total_value
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLER_ORDER_CUSTOMER_METRICS
        GROUP BY seller_tier, category
        ORDER BY total_value DESC
    """).to_pandas()
    st.dataframe(combo, hide_index=True)

with tab5:
    st.subheader("Customer Forecast")

    cf_kpi = session.sql("""
        SELECT
            COUNT(*) AS total_customers,
            SUM(CASE WHEN CHURN_PREDICTION_30D IN ('VERY HIGH','HIGH') THEN 1 ELSE 0 END) AS high_churn_count,
            ROUND(AVG(CHURN_PROBABILITY_SCORE), 3) AS avg_churn_prob,
            ROUND(SUM(REVENUE_AT_RISK), 0) AS total_revenue_at_risk,
            SUM(CASE WHEN CURRENT_LOYALTY_TIER != PREDICTED_LOYALTY_TIER THEN 1 ELSE 0 END) AS tier_migration_count,
            SUM(CASE WHEN RETENTION_PRIORITY = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK
    """).to_pandas()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        with st.container(border=True):
            st.metric("High Churn Risk", int(cf_kpi['HIGH_CHURN_COUNT'][0]))
    with c2:
        with st.container(border=True):
            st.metric("Revenue at Risk", f"${float(cf_kpi['TOTAL_REVENUE_AT_RISK'][0]):,.0f}")
    with c3:
        with st.container(border=True):
            st.metric("Tier Migrations", int(cf_kpi['TIER_MIGRATION_COUNT'][0]))
    with c4:
        with st.container(border=True):
            st.metric("Critical Priority", int(cf_kpi['CRITICAL_COUNT'][0]))

    col_left, col_right = st.columns(2)
    with col_left:
        with st.container(border=True):
            st.write("**Churn prediction (30-day)**")
            churn_dist = session.sql("""
                SELECT CHURN_PREDICTION_30D AS churn_level, COUNT(*) AS count
                FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK
                GROUP BY CHURN_PREDICTION_30D ORDER BY count DESC
            """).to_pandas()
            st.bar_chart(churn_dist, x="CHURN_LEVEL", y="COUNT")

    with col_right:
        with st.container(border=True):
            st.write("**Retention priority breakdown**")
            ret_dist = session.sql("""
                SELECT RETENTION_PRIORITY AS priority, COUNT(*) AS count
                FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK
                GROUP BY RETENTION_PRIORITY ORDER BY count DESC
            """).to_pandas()
            st.bar_chart(ret_dist, x="PRIORITY", y="COUNT")

    col_left2, col_right2 = st.columns(2)
    with col_left2:
        with st.container(border=True):
            st.write("**Loyalty tier migration**")
            tier_mig = session.sql("""
                SELECT CURRENT_LOYALTY_TIER || ' -> ' || PREDICTED_LOYALTY_TIER AS migration,
                       COUNT(*) AS count
                FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK
                WHERE CURRENT_LOYALTY_TIER != PREDICTED_LOYALTY_TIER
                GROUP BY migration ORDER BY count DESC LIMIT 10
            """).to_pandas()
            if len(tier_mig) > 0:
                st.bar_chart(tier_mig, x="MIGRATION", y="COUNT")
            else:
                st.info("No tier migrations predicted.")

    with col_right2:
        with st.container(border=True):
            st.write("**Persona stability**")
            ps_dist = session.sql("""
                SELECT PERSONA_STABILITY AS stability, COUNT(*) AS count
                FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK
                GROUP BY PERSONA_STABILITY ORDER BY count DESC
            """).to_pandas()
            st.bar_chart(ps_dist, x="STABILITY", y="COUNT")

    with st.container(border=True):
        st.write("**Recommended interventions**")
        interv = session.sql("""
            SELECT RECOMMENDED_INTERVENTION AS intervention, COUNT(*) AS count,
                   ROUND(SUM(REVENUE_AT_RISK), 0) AS total_revenue_at_risk
            FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK
            GROUP BY RECOMMENDED_INTERVENTION ORDER BY total_revenue_at_risk DESC
        """).to_pandas()
        st.dataframe(interv, hide_index=True, column_config={
            "INTERVENTION": "Intervention",
            "COUNT": "Customers",
            "TOTAL_REVENUE_AT_RISK": st.column_config.NumberColumn("Revenue at Risk", format="$%.0f"),
        })

    with st.container(border=True):
        st.write("**Customer forecast details**")
        cf_filter = st.selectbox("Filter by retention priority", ["ALL", "CRITICAL", "HIGH", "MEDIUM", "LOW"], key="cf_filter")
        filter_clause = f"WHERE RETENTION_PRIORITY = '{cf_filter}'" if cf_filter != "ALL" else ""
        cf_detail = session.sql(f"""
            SELECT CUSTOMER_ID, CUSTOMER_NAME, PERSONA,
                   CURRENT_LOYALTY_TIER, PREDICTED_LOYALTY_TIER,
                   CURRENT_RFM_SEGMENT, PREDICTED_RFM_SEGMENT,
                   CHURN_PREDICTION_30D, ROUND(CHURN_PROBABILITY_SCORE, 2) AS CHURN_PROB,
                   ROUND(CURRENT_CLV, 0) AS CURRENT_CLV,
                   ROUND(PROJECTED_CLV_90D, 0) AS PROJECTED_CLV_90D,
                   ROUND(REVENUE_AT_RISK, 0) AS REVENUE_AT_RISK,
                   RETENTION_PRIORITY, RECOMMENDED_INTERVENTION
            FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_CUSTOMER_OUTLOOK
            {filter_clause}
            ORDER BY REVENUE_AT_RISK DESC LIMIT 50
        """).to_pandas()
        st.dataframe(cf_detail, hide_index=True, column_config={
            "CUSTOMER_ID": "ID",
            "CUSTOMER_NAME": "Name",
            "PERSONA": "Persona",
            "CURRENT_LOYALTY_TIER": "Tier",
            "PREDICTED_LOYALTY_TIER": "Pred. Tier",
            "CURRENT_RFM_SEGMENT": "RFM",
            "PREDICTED_RFM_SEGMENT": "Pred. RFM",
            "CHURN_PREDICTION_30D": "Churn Risk",
            "CHURN_PROB": st.column_config.ProgressColumn("Churn Prob", min_value=0, max_value=1),
            "CURRENT_CLV": st.column_config.NumberColumn("CLV", format="$%.0f"),
            "PROJECTED_CLV_90D": st.column_config.NumberColumn("CLV 90d", format="$%.0f"),
            "REVENUE_AT_RISK": st.column_config.NumberColumn("Rev at Risk", format="$%.0f"),
            "RETENTION_PRIORITY": "Priority",
            "RECOMMENDED_INTERVENTION": "Intervention",
        })

with tab6:
    st.subheader("Seller Forecast")

    sf_kpi = session.sql("""
        SELECT
            COUNT(*) AS total_sellers,
            SUM(CASE WHEN RISK_FORECAST IN ('CRITICAL','HIGH') THEN 1 ELSE 0 END) AS high_risk_count,
            ROUND(SUM(REVENUE_AT_RISK), 0) AS total_revenue_at_risk,
            ROUND(AVG(SUSTAINABILITY_SCORE), 3) AS avg_sustainability,
            SUM(CASE WHEN CURRENT_SELLER_TIER != PREDICTED_SELLER_TIER THEN 1 ELSE 0 END) AS tier_migration_count,
            SUM(CASE WHEN INVESTMENT_RECOMMENDATION = 'INVEST HEAVILY' THEN 1 ELSE 0 END) AS invest_count
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK
    """).to_pandas()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        with st.container(border=True):
            st.metric("High Risk Sellers", int(sf_kpi['HIGH_RISK_COUNT'][0]))
    with c2:
        with st.container(border=True):
            st.metric("Revenue at Risk", f"${float(sf_kpi['TOTAL_REVENUE_AT_RISK'][0]):,.0f}")
    with c3:
        with st.container(border=True):
            st.metric("Tier Migrations", int(sf_kpi['TIER_MIGRATION_COUNT'][0]))
    with c4:
        with st.container(border=True):
            st.metric("Avg Sustainability", f"{float(sf_kpi['AVG_SUSTAINABILITY'][0]):.1%}")

    col_left, col_right = st.columns(2)
    with col_left:
        with st.container(border=True):
            st.write("**Risk forecast distribution**")
            risk_dist = session.sql("""
                SELECT RISK_FORECAST AS risk_level, COUNT(*) AS count
                FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK
                GROUP BY RISK_FORECAST ORDER BY count DESC
            """).to_pandas()
            st.bar_chart(risk_dist, x="RISK_LEVEL", y="COUNT")

    with col_right:
        with st.container(border=True):
            st.write("**Investment recommendations**")
            inv_dist = session.sql("""
                SELECT INVESTMENT_RECOMMENDATION AS recommendation, COUNT(*) AS count
                FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK
                GROUP BY INVESTMENT_RECOMMENDATION ORDER BY count DESC
            """).to_pandas()
            st.bar_chart(inv_dist, x="RECOMMENDATION", y="COUNT")

    col_left2, col_right2 = st.columns(2)
    with col_left2:
        with st.container(border=True):
            st.write("**Revenue growth trend**")
            rev_trend = session.sql("""
                SELECT REVENUE_GROWTH_TREND AS trend, COUNT(*) AS count,
                       ROUND(AVG(PROJECTED_REVENUE_90D), 0) AS avg_projected_rev
                FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK
                GROUP BY REVENUE_GROWTH_TREND ORDER BY avg_projected_rev DESC
            """).to_pandas()
            st.bar_chart(rev_trend, x="TREND", y="COUNT")

    with col_right2:
        with st.container(border=True):
            st.write("**Operational health forecast**")
            ops_dist = session.sql("""
                SELECT OPERATIONAL_HEALTH_FORECAST AS health, COUNT(*) AS count
                FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK
                GROUP BY OPERATIONAL_HEALTH_FORECAST ORDER BY count DESC
            """).to_pandas()
            st.bar_chart(ops_dist, x="HEALTH", y="COUNT")

    with st.container(border=True):
        st.write("**Recommended actions by revenue at risk**")
        actions = session.sql("""
            SELECT RECOMMENDED_ACTION AS action, COUNT(*) AS count,
                   ROUND(SUM(REVENUE_AT_RISK), 0) AS total_revenue_at_risk,
                   ROUND(AVG(SUSTAINABILITY_SCORE), 3) AS avg_sustainability
            FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK
            GROUP BY RECOMMENDED_ACTION ORDER BY total_revenue_at_risk DESC
        """).to_pandas()
        st.dataframe(actions, hide_index=True, column_config={
            "ACTION": "Action",
            "COUNT": "Sellers",
            "TOTAL_REVENUE_AT_RISK": st.column_config.NumberColumn("Revenue at Risk", format="$%.0f"),
            "AVG_SUSTAINABILITY": st.column_config.ProgressColumn("Avg Sustainability", min_value=0, max_value=1),
        })

    with st.container(border=True):
        st.write("**Seller forecast details**")
        sf_filter = st.selectbox("Filter by risk forecast", ["ALL", "CRITICAL", "HIGH", "ELEVATED", "STABLE"], key="sf_filter")
        sf_clause = f"WHERE RISK_FORECAST = '{sf_filter}'" if sf_filter != "ALL" else ""
        sf_detail = session.sql(f"""
            SELECT SELLER_ID, SELLER_NAME, SELLER_TYPE, SELLER_CATEGORY,
                   CURRENT_SELLER_TIER, PREDICTED_SELLER_TIER,
                   ROUND(CURRENT_REVENUE, 0) AS CURRENT_REVENUE,
                   ROUND(PROJECTED_REVENUE_90D, 0) AS PROJECTED_REV_90D,
                   REVENUE_GROWTH_TREND,
                   RISK_FORECAST,
                   ROUND(OPERATIONAL_HEALTH_SCORE, 1) AS OPS_HEALTH,
                   PROFIT_TRAJECTORY,
                   ROUND(SUSTAINABILITY_SCORE, 2) AS SUSTAINABILITY,
                   ROUND(REVENUE_AT_RISK, 0) AS REVENUE_AT_RISK,
                   INVESTMENT_RECOMMENDATION, RECOMMENDED_ACTION
            FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK
            {sf_clause}
            ORDER BY REVENUE_AT_RISK DESC LIMIT 50
        """).to_pandas()
        st.dataframe(sf_detail, hide_index=True, column_config={
            "SELLER_ID": "ID",
            "SELLER_NAME": "Name",
            "SELLER_TYPE": "Type",
            "SELLER_CATEGORY": "Category",
            "CURRENT_SELLER_TIER": "Tier",
            "PREDICTED_SELLER_TIER": "Pred. Tier",
            "CURRENT_REVENUE": st.column_config.NumberColumn("Revenue", format="$%.0f"),
            "PROJECTED_REV_90D": st.column_config.NumberColumn("Rev 90d", format="$%.0f"),
            "REVENUE_GROWTH_TREND": "Growth",
            "RISK_FORECAST": "Risk",
            "OPS_HEALTH": st.column_config.ProgressColumn("Ops Health", min_value=0, max_value=100),
            "PROFIT_TRAJECTORY": "Margin",
            "SUSTAINABILITY": st.column_config.ProgressColumn("Sustain.", min_value=0, max_value=1),
            "REVENUE_AT_RISK": st.column_config.NumberColumn("Rev at Risk", format="$%.0f"),
            "INVESTMENT_RECOMMENDATION": "Investment",
            "RECOMMENDED_ACTION": "Action",
        })

with tab7:
    st.subheader("AI Data Assistant")
    st.caption("Ask questions and get real data, insights, and SQL queries")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "dataframe" in msg:
                st.dataframe(msg["dataframe"], hide_index=True, use_container_width=True)

    if user_input := st.chat_input("Ask about your e-commerce data..."):
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("Generating query..."):
                safe_input = user_input.replace("'", "''").replace("\\", "\\\\")

                sql_prompt = f"""You are a Snowflake SQL expert for an e-commerce analytics platform.

Database: SNOWFLAKE_LEARNING_DB.PUBLIC

Tables and columns:
- CUSTOMERS: customer_id, first_name, last_name, city, state, loyalty_tier, total_orders, customer_lifetime_value, age, gender
- ORDERS: order_id, customer_id, product_id, order_value, category, order_date, payment_type, delivery_status (NOTE: NO seller_id here - use CUSTOMER_SELLER_MAPPING to link orders to sellers)
- SELLERS: seller_id, seller_name, seller_tier, seller_category, seller_type, total_revenue, total_orders, avg_order_value, seller_score, avg_customer_rating, return_rate_pct, cancellation_rate_pct, fraud_flag_rate
- CUSTOMER_AI_360: customer_id, first_name, last_name, persona, loyalty_tier, churn_risk_score, fraud_risk_score, total_orders, total_order_value, profile_clv, profile_engagement_score, discount_sensitivity_score, return_behavior_score
- RFM_SEGMENTS: customer_id, r_score, f_score, m_score, rfm_score, extracted_value (segment name like Champion, At Risk, etc.)
- FORECAST_CUSTOMER_OUTLOOK (alias: fco): customer_id, customer_name, persona, current_loyalty_tier, predicted_loyalty_tier, current_rfm_segment, predicted_rfm_segment, churn_prediction_30d (STRING: LOW/MEDIUM/HIGH/VERY HIGH), churn_probability_score (FLOAT), current_clv (FLOAT), projected_clv_90d (FLOAT), revenue_at_risk (FLOAT), retention_priority (STRING: CRITICAL/HIGH/MEDIUM/LOW), recommended_intervention (STRING), upsell_potential_score (FLOAT), persona_stability (STRING). NOTE: This table does NOT have risk_forecast, operational_health_score, sustainability_score, or any seller columns.
- FORECAST_SELLER_OUTLOOK (alias: fso): seller_id, seller_name, seller_type, seller_category, current_seller_tier, predicted_seller_tier, current_revenue (FLOAT), projected_revenue_90d (FLOAT), revenue_growth_trend (STRING), risk_forecast (STRING: CRITICAL/HIGH/ELEVATED/STABLE), operational_health_score (FLOAT), operational_health_forecast (STRING), profit_trajectory (STRING), sustainability_score (FLOAT), revenue_at_risk (FLOAT), investment_recommendation (STRING), recommended_action (STRING). NOTE: This table does NOT have churn_prediction_30d, retention_priority, persona, or any customer columns.- CUSTOMER_SELLER_MAPPING: customer_id, order_id, product_id, category, seller_id
- SELLER_ORDER_CUSTOMER_METRICS: seller_id, seller_name, seller_tier, category, order_value, customer_id

RULES:
- Always use fully qualified table names: SNOWFLAKE_LEARNING_DB.PUBLIC.<table>
- Return ONLY a single valid SQL query, nothing else. No explanation, no markdown, no backticks.
- LIMIT results to 20 rows max unless the user asks for aggregates/counts.
- For aggregated questions, group and round appropriately.
- CRITICAL: Always JOIN tables using ID columns (seller_id INT, customer_id INT, order_id INT), NEVER join on name columns. seller_id and customer_id are always INT type.
- When filtering by seller or customer, use the ID column (INT), not the name column (STRING). Example: WHERE seller_id = 144, NOT WHERE seller_name = 'Seller_144'.

IMPORTANT RELATIONSHIPS:
- To connect ORDERS to SELLERS, you MUST join through CUSTOMER_SELLER_MAPPING (it has customer_id, order_id, and seller_id)
- ORDERS does NOT have a seller_id column
- CUSTOMER_SELLER_MAPPING is the ONLY bridge between customers/orders and sellers

User question: {safe_input}"""

                sql_response = session.sql(
                    "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ?) AS response",
                    params=[sql_prompt]
                ).collect()[0]['RESPONSE']

                generated_sql = sql_response.strip().strip('`').strip()
                if generated_sql.upper().startswith("```"):
                    generated_sql = generated_sql.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

            result_df = None
            error_msg = None
            try:
                with st.spinner("Running query..."):
                    result_df = session.sql(generated_sql).to_pandas()
            except Exception as e:
                error_msg = str(e)

            if error_msg:
                with st.spinner("Retrying with fixed query..."):
                    fix_prompt = f"""The following SQL query failed with error: {error_msg}

Original query: {generated_sql}

IMPORTANT COLUMN TYPES:
- String columns: loyalty_tier, persona, churn_prediction_30d, retention_priority, recommended_intervention, persona_stability, current_rfm_segment, predicted_rfm_segment, seller_tier, seller_category, seller_type, risk_forecast, revenue_growth_trend, operational_health_forecast, profit_trajectory, investment_recommendation, recommended_action, payment_method, delivery_status, category
- Numeric columns: churn_risk_score, fraud_risk_score, churn_probability_score, current_clv, projected_clv_90d, revenue_at_risk, upsell_potential_score, seller_score, return_rate_pct, operational_health_score, sustainability_score, current_revenue, projected_revenue_90d, order_value, total_orders, total_revenue, customer_lifetime_value, profile_clv

Do NOT compare string values to numeric columns or vice versa.
Fix the query. Return ONLY the corrected SQL, nothing else. No explanation, no markdown, no backticks.
Use fully qualified names: SNOWFLAKE_LEARNING_DB.PUBLIC.<table>"""
                    fixed_sql = session.sql(
                        "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ?) AS response",
                        params=[fix_prompt]
                    ).collect()[0]['RESPONSE'].strip().strip('`').strip()
                    if fixed_sql.upper().startswith("```"):
                        fixed_sql = fixed_sql.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
                    try:
                        result_df = session.sql(fixed_sql).to_pandas()
                        generated_sql = fixed_sql
                    except Exception as e2:
                        error_msg = str(e2)

            if result_df is not None and len(result_df) > 0:
                with st.spinner("Analyzing results..."):
                    result_preview = result_df.head(20).to_string(index=False)

                    insight_prompt = f"""You are an e-commerce data analyst. The user asked: "{safe_input}"

Here is the actual data from the query:

{result_preview}

Provide a concise analysis with:
1. **Key Findings**: 2-3 bullet points highlighting the most important insights from this data
2. **Business Implications**: 1-2 sentences on what this means for the business
3. **Recommendation**: 1 actionable recommendation based on the data

Be specific - reference actual numbers and values from the data. Format in markdown."""

                    insights = session.sql(
                        "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ?) AS response",
                        params=[insight_prompt]
                    ).collect()[0]['RESPONSE']

                with st.expander("SQL Query", expanded=False):
                    st.code(generated_sql, language="sql")

                st.dataframe(result_df, hide_index=True, use_container_width=True)
                st.caption(f"{len(result_df)} rows returned")
                st.divider()
                st.markdown(insights)

                msg_content = f"**Query executed successfully** - {len(result_df)} rows returned\n\n{insights}"
                st.session_state.chat_history.append({
                    "role": "assistant",
                    "content": msg_content,
                    "dataframe": result_df
                })

            elif result_df is not None and len(result_df) == 0:
                st.info("Query returned no results.")
                with st.expander("SQL Query", expanded=True):
                    st.code(generated_sql, language="sql")
                st.session_state.chat_history.append({
                    "role": "assistant",
                    "content": "Query returned no results. Try rephrasing your question."
                })
            else:
                st.error(f"Could not execute query. Error: {error_msg}")
                with st.expander("Attempted SQL", expanded=True):
                    st.code(generated_sql, language="sql")
                st.session_state.chat_history.append({
                    "role": "assistant",
                    "content": f"Sorry, I couldn't get the data. Error: {error_msg}"
                })

    if st.session_state.chat_history:
        if st.button("Clear chat", key="clear_chat"):
            st.session_state.chat_history = []
            st.rerun()

with tab8:
    st.subheader("Email Alert Center")
    st.caption("Live alerts powered by real-time Dynamic Tables — auto-refreshes with new data")

    col1, col2, col3 = st.columns(3)
    pipeline_stats = session.sql("""
        SELECT
            (SELECT COUNT(*) FROM SNOWFLAKE_LEARNING_DB.PUBLIC.REALTIME_ORDER_EVENTS) AS TOTAL_EVENTS,
            (SELECT MAX(EVENT_TIMESTAMP) FROM SNOWFLAKE_LEARNING_DB.PUBLIC.REALTIME_ORDER_EVENTS) AS LAST_EVENT,
            (SELECT COUNT(*) FROM SNOWFLAKE_LEARNING_DB.PUBLIC.ORDERS) AS TOTAL_ORDERS
    """).to_pandas()
    col1.metric("Total Real-Time Events", f"{pipeline_stats['TOTAL_EVENTS'].iloc[0]:,}")
    col2.metric("Last Event", str(pipeline_stats['LAST_EVENT'].iloc[0])[:19] if pipeline_stats['LAST_EVENT'].iloc[0] else "N/A")
    col3.metric("Total Orders (Base)", f"{pipeline_stats['TOTAL_ORDERS'].iloc[0]:,}")

    st.divider()

    st.write("**11 alerts monitored** — sourced from real-time Dynamic Tables where available")

    alert_preview = session.sql("""
        SELECT 'Seller Fraud (>5%)' AS alert, COUNT(*) AS count
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS WHERE fraud_flag_rate > 5
        UNION ALL
        SELECT 'Customer Churn (HIGH+)', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_FORECAST_CUSTOMER_OUTLOOK
        WHERE REALTIME_CHURN_PREDICTION IN ('VERY HIGH','HIGH')
        UNION ALL
        SELECT 'Critical Retention', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_FORECAST_CUSTOMER_OUTLOOK
        WHERE REALTIME_RETENTION_PRIORITY = 'CRITICAL'
        UNION ALL
        SELECT 'Predictions Changed (Customers)', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_FORECAST_CUSTOMER_OUTLOOK
        WHERE PREDICTION_CHANGED = TRUE
        UNION ALL
        SELECT 'Seller Risk (HIGH+)', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_FORECAST_SELLER_OUTLOOK
        WHERE REALTIME_RISK_FORECAST IN ('CRITICAL','HIGH')
        UNION ALL
        SELECT 'Seller Predictions Changed', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_FORECAST_SELLER_OUTLOOK
        WHERE PREDICTION_CHANGED = TRUE
        UNION ALL
        SELECT 'Ops Health AT RISK', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK
        WHERE OPERATIONAL_HEALTH_FORECAST = 'AT RISK'
        UNION ALL
        SELECT 'High RTO (>10%)', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS WHERE rto_rate_pct > 10
        UNION ALL
        SELECT 'Inventory Stock-Out (>15%)', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS WHERE stock_out_rate > 15
        UNION ALL
        SELECT 'COD Dependency (>60%)', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SELLERS WHERE cod_users_pct > 60
        UNION ALL
        SELECT 'Low Sustainability (<0.4)', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.FORECAST_SELLER_OUTLOOK
        WHERE sustainability_score < 0.4 AND current_revenue > 100000
    """).to_pandas()
    st.dataframe(alert_preview, hide_index=True, use_container_width=True, column_config={
        "ALERT": "Alert Type",
        "COUNT": st.column_config.NumberColumn("Triggered Count"),
    })

    st.divider()

    col_a, col_b = st.columns(2)
    with col_a:
        st.write("**Real-Time Customer Alerts (Top 10)**")
        cust_alerts = session.sql("""
            SELECT CUSTOMER_ID, CUSTOMER_NAME,
                   BASELINE_CHURN_PREDICTION AS BEFORE,
                   REALTIME_CHURN_PREDICTION AS NOW,
                   ROUND(REALTIME_CLV, 0) AS REALTIME_CLV,
                   REALTIME_RETENTION_PRIORITY AS PRIORITY
            FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_FORECAST_CUSTOMER_OUTLOOK
            WHERE PREDICTION_CHANGED = TRUE
            ORDER BY REALTIME_CLV DESC
            LIMIT 10
        """).to_pandas()
        st.dataframe(cust_alerts, hide_index=True, use_container_width=True)

    with col_b:
        st.write("**Real-Time Seller Alerts (Top 10)**")
        seller_alerts = session.sql("""
            SELECT SELLER_ID, SELLER_NAME,
                   BASELINE_RISK_FORECAST AS BEFORE,
                   REALTIME_RISK_FORECAST AS NOW,
                   ROUND(REALTIME_REVENUE, 0) AS REALTIME_REV,
                   REALTIME_ORDER_COUNT AS NEW_ORDERS
            FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_FORECAST_SELLER_OUTLOOK
            WHERE PREDICTION_CHANGED = TRUE
            ORDER BY REALTIME_ORDER_VALUE DESC
            LIMIT 10
        """).to_pandas()
        st.dataframe(seller_alerts, hide_index=True, use_container_width=True)

    st.divider()

    if st.button("Send Email Alert Report Now", type="primary"):
        with st.spinner("Checking alerts and sending email..."):
            result = session.sql("CALL SNOWFLAKE_LEARNING_DB.PUBLIC.SP_CHECK_AND_SEND_ALERTS()").collect()
            st.success(result[0][0])

    with st.container(border=True):
        st.write("**Scheduled Alerts**")
        st.info("Daily email alert task is configured for **9:00 AM IST**. Run this SQL to activate:")
        st.code("ALTER TASK SNOWFLAKE_LEARNING_DB.PUBLIC.TASK_DAILY_EMAIL_ALERTS RESUME;", language="sql")