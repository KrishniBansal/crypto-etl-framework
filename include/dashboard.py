import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine

# 1. Database Connection (using localhost for Windows-to-Docker access)
def get_data():
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres")
    # We pull the data but will handle nulls in the transformation step below
    # Change your query to DESC (Descending) to get the newest prices first
    query = """
        SELECT * FROM bitcoin_gold_metrics 
        ORDER BY trade_date DESC 
    """
    return pd.read_sql(query, engine)

# 2. Styling & Config
st.set_page_config(page_title="Crypto ETL Pro", layout="wide", page_icon="🪙")

# Custom CSS for a professional look
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    div[data-testid="metric-container"] {
        background-color: #161b22;
        border: 1px solid #30363d;
        padding: 15px;
        border-radius: 10px;
    }
    </style>
    """, unsafe_allow_html=True)

st.title("🪙 Enterprise Bitcoin Analytics")
st.markdown("---")

try:
    df = get_data()
    
    if df.empty:
        st.warning("⚠️ The Gold Layer table is empty. Please run your Airflow DAG to generate metrics.")
    else:
        # DATA CLEANING: Convert dates and handle nulls for the Moving Average
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        # Plotly can crash on NaN values; we fill or drop them for the chart specifically
        # 'rolling_7d_avg' will be null for the first 6 rows of a new dataset
        plot_df = df.copy().sort_values('trade_date')

        # --- ROW 1: KEY PERFORMANCE INDICATORS ---
        
        # Since SQL is ORDER BY trade_date DESC:
        # Index 0 is the NEWEST (2026)
        # Index 1 is the PREVIOUS day
        if not df.empty:
            latest = df.iloc[0]
            if len(df) > 1:
                prev = df.iloc[1]
                price_diff = latest['avg_price'] - prev['avg_price']
                vol_diff = latest['daily_volatility_pct'] - prev['daily_volatility_pct']
            else:
                price_diff = 0
                vol_diff = 0

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Current Price", f"${latest['avg_price']:,.2f}", f"{price_diff:,.2f}")
            
            # Check if moving average exists yet (it won't for the first 6 days)
            m_avg = latest['rolling_7d_avg']
            col2.metric("7D Moving Average", f"${m_avg:,.2f}" if pd.notnull(m_avg) else "Calculating...")
            
            col3.metric("Volatility Index", f"{latest['daily_volatility_pct']:.2f}%", f"{vol_diff:.2f}%", delta_color="inverse")
            col4.metric("Total Records", f"{len(df)} Days")
        else:
            st.error("No data found in Gold Metrics.")
        
        
        
        # --- ROW 2: INTERACTIVE PRICE CHART ---
        st.subheader("Price Movement & 7-Day Trend")
        
        fig = go.Figure()
        
        # Daily Average Line
        fig.add_trace(go.Scatter(
            x=plot_df['trade_date'], 
            y=plot_df['avg_price'], 
            name='Daily Avg', 
            line=dict(color='#00d4ff', width=2)
        ))
        
        # Moving Average Line (Only plot rows where it's not null)
        ma_df = plot_df.dropna(subset=['rolling_7d_avg'])
        if not ma_df.empty:
            fig.add_trace(go.Scatter(
                x=ma_df['trade_date'], 
                y=ma_df['rolling_7d_avg'], 
                name='7D Trend', 
                line=dict(color='#ff9f1c', width=2, dash='dash')
            ))
        
        fig.update_layout(
            template="plotly_dark", 
            hovermode="x unified", 
            height=450,
            margin=dict(l=20, r=20, t=30, b=20)
        )
        st.plotly_chart(fig, use_container_width=True)

        # --- ROW 3: DETAILED BREAKDOWN & DATA ---
        c1, c2 = st.columns([2, 1])
        with c1:
            st.subheader("Volatility Trend")
            st.area_chart(df.set_index('trade_date')['daily_volatility_pct'], color="#ff4b4b")
        
        with c2:
            st.subheader("Historical Snapshot")
            # Format the dataframe for readability
            st.dataframe(
                df.tail(10)[['trade_date', 'avg_price', 'rolling_7d_avg']].style.format({
                    'avg_price': '{:.2f}',
                    'rolling_7d_avg': '{:.2f}'
                }), 
                use_container_width=True
            )

except Exception as e:
    st.error(f"⚠️ Dashboard Error: {e}")
    st.info("Ensure Docker is running and your connection string uses 'localhost'.")