import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

if "page" not in st.session_state:
    st.session_state.page = "home"

def go_to(page):
    st.session_state.page = page

PAGES = [
    {"key": "customers", "label": "Customers", "desc": "Loyalty tiers & top CLV"},
    {"key": "analytics", "label": "Analytics", "desc": "Orders by tier & category"},
    {"key": "customer_forecast", "label": "Customer Forecast", "desc": "Churn & retention"},
    {"key": "seller_forecast", "label": "Seller Forecast", "desc": "Risk & sustainability scores"},
    {"key": "ai_chat", "label": "AI Chat", "desc": "Ask data questions"},
    {"key": "email_alerts", "label": "Email Alerts", "desc": "Real-time alert center"},
    {"key": "inventory", "label": "Inventory Alerts", "desc": "Stockout warnings"},
    {"key": "cross_platform", "label": "Cross-Platform", "desc": "Identity & segments"},
    {"key": "self_serve", "label": "Self-Serve", "desc": "Customer portal"},
]

if st.session_state.page == "home":
    st.title("E-Commerce Data Explorer")
    st.caption("Select a section to explore")

    row1 = st.columns(3)
    for i in range(3):
        with row1[i]:
            p = PAGES[i]
            with st.container(border=True):
                st.write(f"**{p['label']}**")
                st.caption(p["desc"])
                st.button("Open", key=f"btn_{p['key']}", on_click=go_to, args=(p["key"],), use_container_width=True)

    row2 = st.columns(3)
    for i in range(3, 6):
        with row2[i - 3]:
            p = PAGES[i]
            with st.container(border=True):
                st.write(f"**{p['label']}**")
                st.caption(p["desc"])
                st.button("Open", key=f"btn_{p['key']}", on_click=go_to, args=(p["key"],), use_container_width=True)

    row3 = st.columns(3)
    for i in range(6, 9):
        with row3[i - 6]:
            p = PAGES[i]
            with st.container(border=True):
                st.write(f"**{p['label']}**")
                st.caption(p["desc"])
                st.button("Open", key=f"btn_{p['key']}", on_click=go_to, args=(p["key"],), use_container_width=True)

elif st.session_state.page == "customers":
    st.button(":material/arrow_back: Home", on_click=go_to, args=("home",))
    st.title("E-Commerce Data Explorer")
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

elif st.session_state.page == "analytics":
    st.button(":material/arrow_back: Home", on_click=go_to, args=("home",))
    st.title("E-Commerce Data Explorer")
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

elif st.session_state.page == "customer_forecast":
    st.button(":material/arrow_back: Home", on_click=go_to, args=("home",))
    st.title("E-Commerce Data Explorer")
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

elif st.session_state.page == "seller_forecast":
    st.button(":material/arrow_back: Home", on_click=go_to, args=("home",))
    st.title("E-Commerce Data Explorer")
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

elif st.session_state.page == "ai_chat":
    st.button(":material/arrow_back: Home", on_click=go_to, args=("home",))
    st.title("E-Commerce Data Explorer")
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
- ORDERS: order_id, customer_id, product_id, order_value, category, order_date, payment_type, order_status (NOTE: NO seller_id here - use CUSTOMER_SELLER_MAPPING to link orders to sellers)
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

elif st.session_state.page == "email_alerts":
    st.button(":material/arrow_back: Home", on_click=go_to, args=("home",))
    st.title("E-Commerce Data Explorer")
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

    st.write("**20 alerts monitored** — sourced from real-time Dynamic Tables where available")

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
        UNION ALL
        SELECT 'Inventory Stockout (Products)', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_INVENTORY_RISK
        WHERE RISK_LEVEL = 'STOCKOUT'
        UNION ALL
        SELECT 'Inventory Critical (Products)', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_INVENTORY_RISK
        WHERE RISK_LEVEL = 'CRITICAL'
        UNION ALL
        SELECT 'Unfulfillable Orders (Stock Risk)', SUM(ESTIMATED_UNFULFILLABLE_ORDERS)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_INVENTORY_RISK
        WHERE RISK_LEVEL IN ('STOCKOUT', 'CRITICAL')
        UNION ALL
        SELECT 'Revenue At Risk ($)', ROUND(SUM(POTENTIAL_REVENUE_LOSS), 0)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_INVENTORY_RISK
        WHERE RISK_LEVEL IN ('STOCKOUT', 'CRITICAL')
        UNION ALL
        SELECT 'Avg Days to Stockout', ROUND(AVG(CASE WHEN PENDING_ORDERS > 0 THEN CURRENT_STOCK_LEVEL * 1.0 / PENDING_ORDERS ELSE NULL END), 1)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_INVENTORY_RISK
        WHERE RISK_LEVEL IN ('WARNING', 'CRITICAL')
        UNION ALL
        SELECT 'Warning to Critical Soon (Stock < 5)', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_INVENTORY_RISK
        WHERE RISK_LEVEL = 'WARNING' AND STOCK_AFTER_PENDING < 5
        UNION ALL
        SELECT 'Categories Affected by Stockout', COUNT(DISTINCT CATEGORY)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_INVENTORY_RISK
        WHERE RISK_LEVEL IN ('STOCKOUT', 'CRITICAL')
        UNION ALL
        SELECT 'Sellers with 3+ Stockout Products', COUNT(*)
        FROM (SELECT SELLER_ID FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_INVENTORY_RISK WHERE RISK_LEVEL = 'STOCKOUT' GROUP BY SELLER_ID HAVING COUNT(*) >= 3)
        UNION ALL
        SELECT 'Open Inventory Alerts', COUNT(*)
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.INVENTORY_STOCKOUT_ALERTS
        WHERE ALERT_STATUS = 'OPEN'
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

elif st.session_state.page == "inventory":
    st.button(":material/arrow_back: Home", on_click=go_to, args=("home",))
    st.title("E-Commerce Data Explorer")
    st.subheader("📦 Inventory Stockout Early Warning")

    risk_df = session.sql("""
        SELECT RISK_LEVEL, COUNT(*) AS PRODUCTS,
               SUM(ESTIMATED_UNFULFILLABLE_ORDERS) AS UNFULFILLABLE_ORDERS,
               ROUND(SUM(POTENTIAL_REVENUE_LOSS), 0) AS REVENUE_AT_RISK
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_INVENTORY_RISK
        GROUP BY RISK_LEVEL ORDER BY 1
    """).to_pandas()

    c1, c2, c3, c4 = st.columns(4)
    risk_map = {r['RISK_LEVEL']: r for _, r in risk_df.iterrows()}
    c1.metric("Stockout", int(risk_map.get('STOCKOUT', {}).get('PRODUCTS', 0)))
    c2.metric("Critical", int(risk_map.get('CRITICAL', {}).get('PRODUCTS', 0)))
    c3.metric("Warning", int(risk_map.get('WARNING', {}).get('PRODUCTS', 0)))
    c4.metric("Healthy", int(risk_map.get('HEALTHY', {}).get('PRODUCTS', 0)))

    st.subheader("Open Alerts")
    alerts_df = session.sql("""
        SELECT ALERT_TIMESTAMP, SELLER_NAME, PRODUCT_ID, PRODUCT_NAME, CATEGORY,
               CURRENT_STOCK, PENDING_ORDERS, RISK_LEVEL,
               ESTIMATED_STOCKOUT_ORDERS, ROUND(POTENTIAL_REVENUE_LOSS, 0) AS REVENUE_LOSS,
               ALERT_STATUS
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.INVENTORY_STOCKOUT_ALERTS
        WHERE ALERT_STATUS = 'OPEN'
        ORDER BY ALERT_TIMESTAMP DESC LIMIT 50
    """).to_pandas()
    st.dataframe(alerts_df, use_container_width=True, hide_index=True)

    st.subheader("At-Risk Products by Category")
    cat_risk = session.sql("""
        SELECT CATEGORY, RISK_LEVEL, COUNT(*) AS PRODUCTS
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_INVENTORY_RISK
        WHERE RISK_LEVEL IN ('STOCKOUT', 'CRITICAL', 'WARNING')
        GROUP BY CATEGORY, RISK_LEVEL ORDER BY PRODUCTS DESC
    """).to_pandas()
    if len(cat_risk) > 0:
        st.bar_chart(cat_risk, x="CATEGORY", y="PRODUCTS", color="RISK_LEVEL")

    st.subheader("Top Sellers with Stockout Issues")
    seller_risk = session.sql("""
        SELECT SELLER_NAME,
               SUM(CASE WHEN RISK_LEVEL = 'STOCKOUT' THEN 1 ELSE 0 END) AS STOCKOUT_PRODUCTS,
               SUM(CASE WHEN RISK_LEVEL = 'WARNING' THEN 1 ELSE 0 END) AS WARNING_PRODUCTS,
               SUM(ESTIMATED_UNFULFILLABLE_ORDERS) AS TOTAL_UNFULFILLABLE,
               ROUND(SUM(POTENTIAL_REVENUE_LOSS), 0) AS TOTAL_REVENUE_LOSS
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.DT_INVENTORY_RISK
        WHERE RISK_LEVEL != 'HEALTHY'
        GROUP BY SELLER_NAME ORDER BY STOCKOUT_PRODUCTS DESC LIMIT 15
    """).to_pandas()
    st.dataframe(seller_risk, hide_index=True)

elif st.session_state.page == "cross_platform":
    st.button(":material/arrow_back: Home", on_click=go_to, args=("home",))
    st.title("E-Commerce Data Explorer")
    st.subheader("🌐 Cross-Platform Customer Identity")

    seg_df = session.sql("""
        SELECT CROSS_PLATFORM_SEGMENT, COUNT(*) AS CUSTOMERS,
               ROUND(AVG(TOTAL_SPEND_ALL_PLATFORMS), 0) AS AVG_SPEND,
               ROUND(AVG(PLATFORM_COUNT), 1) AS AVG_PLATFORMS
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.CUSTOMER_CROSS_PLATFORM_IDENTITY
        GROUP BY CROSS_PLATFORM_SEGMENT ORDER BY CUSTOMERS DESC
    """).to_pandas()

    c1, c2, c3, c4 = st.columns(4)
    seg_map = {r['CROSS_PLATFORM_SEGMENT']: r for _, r in seg_df.iterrows()}
    for col, seg, label in [
        (c1, 'OMNICHANNEL_POWER_USER', 'Omnichannel'),
        (c2, 'MULTI_PLATFORM', 'Multi-Platform'),
        (c3, 'DUAL_PLATFORM', 'Dual-Platform'),
        (c4, 'SINGLE_PLATFORM', 'Single-Platform')
    ]:
        row = seg_map.get(seg)
        if row is not None:
            col.metric(label, int(row['CUSTOMERS']), f"${int(row['AVG_SPEND']):,} avg")
        else:
            col.metric(label, 0)

    st.subheader("Marketing Action Segments")
    action_df = session.sql("""
        SELECT MARKETING_ACTION_SEGMENT, COUNT(*) AS CUSTOMERS,
               ROUND(AVG(TOTAL_REVENUE), 0) AS AVG_REVENUE,
               ROUND(AVG(SWITCH_RATE_PCT), 1) AS AVG_SWITCH_RATE
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.CROSS_PLATFORM_MARKETING_INSIGHTS
        GROUP BY MARKETING_ACTION_SEGMENT ORDER BY CUSTOMERS DESC
    """).to_pandas()
    st.bar_chart(action_df, x="MARKETING_ACTION_SEGMENT", y="CUSTOMERS")
    st.dataframe(action_df, hide_index=True)

    st.subheader("High-Value Cross-Platform Customers")
    vip_df = session.sql("""
        SELECT CUSTOMER_NAME, LOYALTY_TIER, CROSS_PLATFORM_SEGMENT,
               PLATFORM_COUNT, TOTAL_ORDERS_ALL_PLATFORMS AS TOTAL_ORDERS,
               ROUND(TOTAL_SPEND_ALL_PLATFORMS, 0) AS TOTAL_SPEND,
               HIGHEST_SPEND_PLATFORM, MOST_FREQUENT_PLATFORM, RFM_SEGMENT
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.CUSTOMER_CROSS_PLATFORM_IDENTITY
        WHERE CROSS_PLATFORM_VALUE = 'HIGH'
        ORDER BY TOTAL_SPEND_ALL_PLATFORMS DESC LIMIT 20
    """).to_pandas()
    st.dataframe(vip_df, use_container_width=True, hide_index=True)

    st.subheader("Campaign Recommendations")
    campaigns_df = session.sql("""
        SELECT MARKETING_ACTION_SEGMENT, RECOMMENDED_CAMPAIGN_ACTION,
               COUNT(*) AS TARGET_CUSTOMERS,
               ROUND(AVG(TOTAL_REVENUE), 0) AS AVG_REVENUE
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.CROSS_PLATFORM_MARKETING_INSIGHTS
        GROUP BY MARKETING_ACTION_SEGMENT, RECOMMENDED_CAMPAIGN_ACTION
        ORDER BY TARGET_CUSTOMERS DESC
    """).to_pandas()
    st.dataframe(campaigns_df, use_container_width=True, hide_index=True)

elif st.session_state.page == "self_serve":
    st.button(":material/arrow_back: Home", on_click=go_to, args=("home",))
    st.title("E-Commerce Data Explorer")
    st.subheader("Customer Self-Serve Portal")
    st.caption("Customers can check their orders, loyalty status, recommendations & more")

    if "cust_chat" not in st.session_state:
        st.session_state.cust_chat = []

    cust_id_input = st.number_input("Enter your Customer ID", min_value=1, max_value=3000, value=1, step=1, key="cust_id")

    cust_info = session.sql(f"""
        SELECT CUSTOMER_ID, FIRST_NAME || ' ' || LAST_NAME AS NAME, CITY, STATE,
               LOYALTY_TIER, LOYALTY_POINTS, ROUND(CUSTOMER_LIFETIME_VALUE, 0) AS CLV,
               TOTAL_ORDERS, PREFERRED_CATEGORY, PREFERRED_PAYMENT_METHOD
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.CUSTOMERS
        WHERE CUSTOMER_ID = {int(cust_id_input)}
    """).to_pandas()

    if len(cust_info) > 0:
        row = cust_info.iloc[0]
        st.write(f"**Welcome, {row['NAME']}!**")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Loyalty Tier", row['LOYALTY_TIER'])
        c2.metric("Points", f"{int(row['LOYALTY_POINTS']):,}")
        c3.metric("Lifetime Value", f"${int(row['CLV']):,}")
        c4.metric("Total Orders", int(row['TOTAL_ORDERS']))

        st.divider()

        quick_c1, quick_c2, quick_c3 = st.columns(3)

        with quick_c1:
            if st.button("My Recent Orders", key="btn_orders"):
                st.session_state.cust_chat.append({"role": "user", "content": "Show my recent orders"})
                orders_df = session.sql(f"""
                    SELECT ORDER_ID, ORDER_DATE, PRODUCT_NAME, CATEGORY, BRAND,
                           ROUND(ORDER_VALUE, 0) AS ORDER_VALUE, ORDER_STATUS,
                           DELIVERY_TIME_DAYS, IS_RETURNED
                    FROM SNOWFLAKE_LEARNING_DB.PUBLIC.ORDERS
                    WHERE CUSTOMER_ID = {int(cust_id_input)}
                    ORDER BY ORDER_DATE DESC LIMIT 10
                """).to_pandas()
                st.session_state.cust_chat.append({"role": "assistant", "content": "Here are your recent orders:", "data": orders_df})

        with quick_c2:
            if st.button("My Loyalty Status", key="btn_loyalty"):
                st.session_state.cust_chat.append({"role": "user", "content": "Show my loyalty status"})
                rfm_df = session.sql(f"""
                    SELECT r.RFM_SEGMENT, r.R_SCORE, r.F_SCORE, r.M_SCORE, r.RFM_SCORE,
                           c.LOYALTY_TIER, c.LOYALTY_POINTS,
                           ROUND(c.CUSTOMER_LIFETIME_VALUE, 0) AS CLV
                    FROM SNOWFLAKE_LEARNING_DB.PUBLIC.RFM_SEGMENTS r
                    JOIN SNOWFLAKE_LEARNING_DB.PUBLIC.CUSTOMERS c ON r.CUSTOMER_ID = c.CUSTOMER_ID
                    WHERE r.CUSTOMER_ID = {int(cust_id_input)}
                """).to_pandas()
                st.session_state.cust_chat.append({"role": "assistant", "content": "Here's your loyalty breakdown:", "data": rfm_df})

        with quick_c3:
            if st.button("My Platform Activity", key="btn_platform"):
                st.session_state.cust_chat.append({"role": "user", "content": "Show my cross-platform activity"})
                plat_df = session.sql(f"""
                    SELECT PLATFORM, CHANNEL, ORDERS_ON_PLATFORM,
                           ROUND(TOTAL_SPEND_ON_PLATFORM, 0) AS SPEND,
                           ROUND(AVG_ORDER_VALUE_ON_PLATFORM, 0) AS AOV,
                           FIRST_ORDER_ON_PLATFORM, LAST_ORDER_ON_PLATFORM
                    FROM SNOWFLAKE_LEARNING_DB.PUBLIC.CUSTOMER_PLATFORM_PROFILES
                    WHERE CUSTOMER_ID = {int(cust_id_input)}
                    ORDER BY TOTAL_SPEND_ON_PLATFORM DESC
                """).to_pandas()
                st.session_state.cust_chat.append({"role": "assistant", "content": "Your activity across platforms:", "data": plat_df})

        st.divider()

        for msg in st.session_state.cust_chat:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
                if "data" in msg and msg["data"] is not None:
                    st.dataframe(msg["data"], hide_index=True, use_container_width=True)

        cust_prompt = st.chat_input("Ask about your orders, returns, recommendations...", key="cust_chat_input")

        if cust_prompt:
            st.session_state.cust_chat.append({"role": "user", "content": cust_prompt})

            with st.spinner("Looking up your info..."):
                safe_prompt = cust_prompt.replace("'", "''")
                cust_sql_prompt = f"""You are a helpful customer service SQL expert. Customer ID is {int(cust_id_input)}.

Available tables in SNOWFLAKE_LEARNING_DB.PUBLIC:
- CUSTOMERS: customer_id, first_name, last_name, city, loyalty_tier, loyalty_points, customer_lifetime_value, total_orders, preferred_category, churn_risk_score, engagement_score
- ORDERS: order_id, customer_id, order_date, product_name, brand, category, order_value, discount_amount, payment_type, order_status, delivery_time_days, is_returned, return_reason, is_cancelled, customer_rating, profit
- RFM_SEGMENTS: customer_id, r_score, f_score, m_score, rfm_score, rfm_segment
- CUSTOMER_PLATFORM_PROFILES: customer_id, platform, channel, orders_on_platform, total_spend_on_platform, avg_order_value_on_platform, categories_bought
- CUSTOMER_CROSS_PLATFORM_IDENTITY: customer_id, platform_count, all_platforms, cross_platform_segment, highest_spend_platform
- CUSTOMER_SELLER_MAPPING: customer_id, order_id, seller_id
- SELLERS: seller_id, seller_name, seller_category, avg_customer_rating

Generate ONLY a SQL query for customer {int(cust_id_input)}.
Always filter with WHERE customer_id = {int(cust_id_input)}.
Use fully qualified names: SNOWFLAKE_LEARNING_DB.PUBLIC.<table>.
LIMIT 20. Return ONLY SQL, no explanation."""

                sql_result = session.sql(
                    "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ?) AS response",
                    params=[cust_sql_prompt + "\n\nQuestion: " + safe_prompt]
                ).collect()[0]['RESPONSE']

                sql_query = sql_result.strip().replace("```sql", "").replace("```", "").strip()

                try:
                    result_df = session.sql(sql_query).to_pandas()

                    summary_prompt = f"""You are a friendly customer service agent. Customer asked: "{safe_prompt}"
Data returned:
{result_df.head(10).to_string()}

Give a helpful, friendly response in 2-3 sentences. Use the customer's name if available. Be specific with numbers."""

                    summary = session.sql(
                        "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ?) AS response",
                        params=[summary_prompt]
                    ).collect()[0]['RESPONSE']

                    st.session_state.cust_chat.append({
                        "role": "assistant",
                        "content": summary,
                        "data": result_df
                    })
                except Exception as e:
                    st.session_state.cust_chat.append({
                        "role": "assistant",
                        "content": f"Sorry, I couldn't find that info. Try asking about your orders, returns, loyalty points, or recommendations.",
                        "data": None
                    })

            st.rerun()

        if st.session_state.cust_chat:
            if st.button("Clear conversation", key="clear_cust_chat"):
                st.session_state.cust_chat = []
                st.rerun()
    else:
        st.warning("Customer ID not found. Please enter a valid ID (1-3000).")
