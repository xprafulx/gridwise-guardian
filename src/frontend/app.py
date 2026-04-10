import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from src.database.connection import get_db_connection
from datetime import datetime

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Gridwise Guardian",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 1. DATA FETCHING: AVAILABLE DATES ---
@st.cache_data(ttl=60)
def get_available_dates(area):
    engine = get_db_connection()
    try:
        query = f"""
            SELECT DISTINCT DATE(forecast_time) as forecast_date 
            FROM forecast_results 
            WHERE price_area = '{area}' 
            ORDER BY forecast_date DESC 
            LIMIT 7
        """
        df = pd.read_sql(query, engine)
        return df['forecast_date'].tolist()
    except Exception as e:
        st.error(f"Database Error: {e}")
        return []

# --- 2. DATA FETCHING: 2-YEAR DYNAMIC THRESHOLDS ---
@st.cache_data(ttl=3600) # Cache this heavy calculation for 1 hour
def get_dynamic_thresholds(area, years_back=2):
    engine = get_db_connection()
    try:
        query = f"""
            SELECT spot_price_dkk_kwh, co2_emissions_g_kwh 
            FROM historical_training_data 
            WHERE price_area = '{area}' 
            AND ds >= NOW() - INTERVAL '{years_back} YEARS'
        """
        df = pd.read_sql(query, engine)
        return {
            'p33_price': df['spot_price_dkk_kwh'].quantile(0.33), 
            'p83_price': df['spot_price_dkk_kwh'].quantile(0.83),   
            'p33_co2': df['co2_emissions_g_kwh'].quantile(0.33),  
            'p83_co2': df['co2_emissions_g_kwh'].quantile(0.83)     
        }
    except Exception as e:
        st.error(f"Stats Error: {e}")
        return None

# --- 3. DATA FETCHING: TIMELINE DATA ---
@st.cache_data(ttl=60) 
def load_forecast_data(area, target_date):
    engine = get_db_connection()
    thresholds = get_dynamic_thresholds(area)
    
    try:
        query = f"""
            SELECT forecast_time, predicted_co2, spot_price_dkk_kwh, generated_at
            FROM forecast_results
            WHERE price_area = '{area}' 
            AND DATE(forecast_time) = '{target_date}'
            ORDER BY forecast_time ASC
        """
        df = pd.read_sql(query, engine)
        
        if not df.empty and thresholds:
            df = df.sort_values('generated_at').drop_duplicates(subset=['forecast_time'], keep='last')
            df = df.sort_values('forecast_time') 
            
            df['forecast_time'] = pd.to_datetime(df['forecast_time'])
            df['hour_display'] = df['forecast_time'].dt.strftime('%H:00')
            
            # --- APPLY UNIFIED 2-YEAR LOGIC FOR UI COLORS ---
            def apply_logic(row):
                price = row['spot_price_dkk_kwh']
                co2 = row['predicted_co2']
                hour = row['forecast_time'].hour
                
                # Red Zone (Worst 17% or Peak Hours)
                if (17 <= hour <= 21) or (price > thresholds['p83_price']) or (co2 > thresholds['p83_co2']):
                    return pd.Series(['rgba(231, 76, 60, 0.8)', 'rgba(231, 76, 60, 0.2)', '🔴 AVOID'])
                # Green Zone (Best 33%)
                elif (price <= thresholds['p33_price']) and (co2 <= thresholds['p33_co2']):
                    return pd.Series(['rgba(46, 204, 113, 0.8)', 'rgba(46, 204, 113, 0.2)', '🟢 BEST'])
                # Yellow Zone
                else:
                    return pd.Series(['rgba(241, 196, 15, 0.8)', 'rgba(241, 196, 15, 0.2)', '🟡 CAUTION'])
            
            # Assign Chart Color, Table Background, and Text Status
            df[['chart_color', 'table_color', 'status']] = df.apply(apply_logic, axis=1)
            
            # Keep a relative score just to pick the #1 best/worst hour for the Top KPI boxes
            df['Danger Score'] = (df['spot_price_dkk_kwh'] / df['spot_price_dkk_kwh'].max()) + \
                                 (df['predicted_co2'] / df['predicted_co2'].max())
            
        return df
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()

# --- SIDEBAR ---
st.sidebar.image("https://img.icons8.com/fluency/96/000000/green-earth.png", width=80)
st.sidebar.title("Gridwise Guardian")
st.sidebar.markdown("Predictive AI for the Danish Power Grid.")

selected_area = st.sidebar.radio("Select Price Area:", ("DK1 (Jutland/Funen)", "DK2 (Zealand/Cph)"))
area_code = selected_area[:3]

available_dates = get_available_dates(area_code)
if available_dates:
    selected_date = st.sidebar.selectbox("📅 Select Date to View:", available_dates)
else:
    selected_date = None

# --- MAIN DASHBOARD ---
if not selected_date:
    st.warning(f"No database records found for {area_code}.")
else:
    df = load_forecast_data(area_code, selected_date)

    if df.empty:
        st.warning("No data available for this date.")
    else:
        st.title(f"⚡ Grid Forecast: {area_code} ({selected_date})")
        
        # --- 1. THE GUARDIAN's MANIFESTO (KPIs) ---
        st.markdown("### 🎯 The Guardian's Manifesto")
        
        best_row = df.loc[df['Danger Score'].idxmin()]
        worst_row = df.loc[df['Danger Score'].idxmax()]
        avg_price = df['spot_price_dkk_kwh'].mean()

        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.success(f"**🟢 Optimal Usage Window**\n\n# {best_row['hour_display']}\n"
                       f"Price: {best_row['spot_price_dkk_kwh']:.2f} kr | CO2: {best_row['predicted_co2']:.0f}g")
        with col2:
            st.error(f"**🔴 Absolute Danger Zone**\n\n# {worst_row['hour_display']}\n"
                     f"Price: {worst_row['spot_price_dkk_kwh']:.2f} kr | CO2: {worst_row['predicted_co2']:.0f}g")
        with col3:
            st.info(f"**📊 Daily Average Price**\n\n# {avg_price:.2f} kr\n"
                    f"Generated at: {pd.to_datetime(df['generated_at'].iloc[0]).strftime('%H:%M')}")

        st.divider()

        # --- 2. THE INTERACTIVE FORECAST CHART ---
        st.markdown("### 📉 24-Hour Timeline")
        
        fig = go.Figure()

        # Add Price as TRAFFIC LIGHT colored bars
        fig.add_trace(go.Bar(
            x=df['hour_display'], 
            y=df['spot_price_dkk_kwh'],
            name='Price/Score Status',
            marker_color=df['chart_color'], 
            yaxis='y1'
        ))

        # Add CO2 as a blue line
        fig.add_trace(go.Scatter(
            x=df['hour_display'], 
            y=df['predicted_co2'],
            name='CO2 (g/kWh)',
            mode='lines+markers',
            line=dict(color='#3498db', width=3),
            yaxis='y2'
        ))

        fig.update_layout(
            xaxis=dict(title='Hour of Day'),
            yaxis=dict(title='Price (DKK/kWh)'),
            yaxis2=dict(title='CO2 Emissions (g/kWh)', titlefont=dict(color='#3498db'), tickfont=dict(color='#3498db'),
                        anchor='x', overlaying='y', side='right'),
            legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0)'),
            margin=dict(l=0, r=0, t=30, b=0),
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

        # --- 3. THE RAW DATA (Color-Coded Table) ---
        with st.expander("🔍 View Raw Database Output"):
            
            # Create the display table WITHOUT the color column
            display_df = df[['hour_display', 'spot_price_dkk_kwh', 'predicted_co2', 'status']].copy()
            display_df.rename(columns={'hour_display': 'Time', 'spot_price_dkk_kwh': 'Price (DKK)', 'predicted_co2': 'CO2 (g/kWh)', 'status': 'Grid Status'}, inplace=True)
            
            # The painter function secretly looks at the original 'df' for the color!
            def color_table_rows(row):
                color = df.loc[row.name, 'table_color']
                return [f'background-color: {color}'] * len(row)
            
            styled_df = display_df.style.apply(color_table_rows, axis=1).format({
                'Price (DKK)': "{:.2f}",
                'CO2 (g/kWh)': "{:.0f}"
            })
            
            st.dataframe(styled_df, use_container_width=True)