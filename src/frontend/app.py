import sys
import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit.components.v1 as components
from src.database.connection import get_db_connection
from datetime import datetime
import pytz  # --- ADDED: Timezone library ---

# --- 1. PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# --- 2. THE "TEMPTATION" UI STYLING ---
def apply_addictive_ui():
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;500;700;900&family=Space+Grotesk:wght@300;700&display=swap');
        
        /* THE ULTIMATE BACKGROUND: Deep Velvet & Neon Depth */
        .stApp {
            background: radial-gradient(circle at 50% -10%, #1A1F26 0%, #05070A 100%);
            color: #F1F5F9;
            font-family: 'Outfit', sans-serif;
        }

        /* TEXT THAT GLOWS */
        h1, h2, h3 {
            font-family: 'Space Grotesk', sans-serif !important;
            font-weight: 700 !important;
            letter-spacing: -0.05em !important;
            color: #FFFFFF !important;
            text-shadow: 0 0 20px rgba(255,255,255,0.1);
        }
        
        /* KPI CARD: Sleek, Dark, and Sharp */
        .kpi-card {
            background: linear-gradient(145deg, rgba(255,255,255,0.03) 0%, rgba(255,255,255,0.01) 100%);
            border: 1px solid rgba(255, 255, 255, 0.04);
            padding: 35px 25px;
            border-radius: 24px;
            text-align: center;
            box-shadow: 0 10px 30px rgba(0,0,0,0.5);
            transition: all 0.5s cubic-bezier(0.4, 0, 0.2, 1);
        }
        .kpi-card:hover {
            border-color: rgba(56, 189, 248, 0.4);
            background: rgba(255, 255, 255, 0.04);
            transform: scale(1.02);
        }
        .kpi-label { font-size: 0.65rem; color: #475569; text-transform: uppercase; letter-spacing: 0.3em; margin-bottom: 15px; }
        .kpi-value { font-size: 2.8rem; font-weight: 900; color: #F8FAFC; line-height: 1; }
        .kpi-sub { font-size: 0.9rem; margin-top: 10px; font-weight: 500; letter-spacing: 0.05em; }

        /* SIDEBAR */
        section[data-testid="stSidebar"] {
            background-color: #05070A !important;
            border-right: 1px solid rgba(255, 255, 255, 0.02);
        }
        </style>
    """, unsafe_allow_html=True)

# --- 3. TICKER LOGIC (Next Action Calculation) ---
def get_ticker_logic(df):
    dk_tz = pytz.timezone('Europe/Copenhagen') # --- ADDED: Danish Timezone ---
    now = datetime.now(dk_tz)
    h = now.hour
    if df.empty or h >= len(df): 
        return "SYNCING PULSE...", "#475569"
    
    status = df.iloc[h]['recommendation_status']
    duration = 0
    # Count consecutive hours of same status
    for i in range(h, len(df)):
        if df.iloc[i]['recommendation_status'] == status:
            duration += 1
        else:
            break
            
    color_map = {'BEST': '#10B981', 'CAUTION': '#F59E0B', 'AVOID': '#EF4444'}
    msg_map = {
        'BEST': f"🟢 SAFE TO CHARGE FOR {duration}H",
        'AVOID': f"🔴 HALT USAGE FOR {duration}H",
        'CAUTION': f"🟡 LIMIT LOAD FOR {duration}H"
    }
    return msg_map.get(status, "GRID SHIFTING..."), color_map.get(status, "#fff")

# --- 4. THE LIVE TICKING CLOCK WITH SECOND HAND & TICKER ---
def render_sidebar_clock(df):
    dk_tz = pytz.timezone('Europe/Copenhagen') # --- ADDED: Danish Timezone ---
    now = datetime.now(dk_tz)
    hour_colors = df['bar_color'].tolist()
    current_color = hour_colors[now.hour] if now.hour < len(hour_colors) else "#444"
    current_status = df['recommendation_status'].iloc[now.hour] if now.hour < len(df) else "SYNCING"
    
    # Get the seductive ticker message
    ticker_text, ticker_color = get_ticker_logic(df)

    clock_html = f"""
    <div style="display:flex; flex-direction:column; align-items:center; font-family:'Space Grotesk';">
        <svg width="170" height="170" viewBox="0 0 200 200">
            <circle cx="100" cy="100" r="95" stroke="rgba(255,255,255,0.02)" stroke-width="1" fill="none" />
            <circle cx="100" cy="100" r="12" fill="{current_color}" style="filter:blur(10px); opacity:0.5;">
                <animate attributeName="opacity" values="0.2;0.6;0.2" dur="3s" repeatCount="indefinite" />
            </circle>
            <circle cx="100" cy="100" r="5" fill="{current_color}" />
            <g id="marks"></g>
            <line id="h" x1="100" y1="100" x2="100" y2="65" stroke="#fff" stroke-width="5" stroke-linecap="round" />
            <line id="m" x1="100" y1="100" x2="100" y2="45" stroke="#38BDF8" stroke-width="3" stroke-linecap="round" />
            <line id="s" x1="100" y1="100" x2="100" y2="35" stroke="#F43F5E" stroke-width="1.5" stroke-linecap="round" />
        </svg>
        
        <div style="margin-top:25px; text-align:center; width:100%;">
            <div style="color:{current_color}; font-weight:700; font-size:10px; letter-spacing:4px; text-transform:uppercase; margin-bottom:8px; opacity:0.8;">{current_status} NOW</div>
            <div style="color:{ticker_color}; font-weight:700; font-size:12px; letter-spacing:1px; text-shadow: 0 0 15px {ticker_color}66; animation: pulse 2s infinite;">
                {ticker_text}
            </div>
        </div>
    </div>
    <style>
        @keyframes pulse {{
            0% {{ opacity: 0.7; }}
            50% {{ opacity: 1; }}
            100% {{ opacity: 0.7; }}
        }}
    </style>
    <script>
        const colors = {str(hour_colors)};
        const g = document.getElementById('marks');
        for (let i=0; i<12; i++) {{
            const a = (i*30)*(Math.PI/180);
            const x = 100+84*Math.sin(a); const y = 100-84*Math.cos(a);
            const h = new Date().getHours();
            const c = document.createElementNS("http://www.w3.org/2000/svg","circle");
            c.setAttribute("cx",x); c.setAttribute("cy",y); c.setAttribute("r","3.5");
            c.setAttribute("fill", colors[h >= 12 ? i+12 : i] || '#111'); g.appendChild(c);
        }}
        function tick() {{
            const d = new Date();
            const hAngle = (d.getHours()%12)*30 + d.getMinutes()*0.5;
            const mAngle = d.getMinutes()*6;
            const sAngle = d.getSeconds()*6;
            document.getElementById('h').setAttribute('transform',`rotate(${{hAngle}},100,100)`);
            document.getElementById('m').setAttribute('transform',`rotate(${{mAngle}},100,100)`);
            document.getElementById('s').setAttribute('transform',`rotate(${{sAngle}},100,100)`);
        }}
        setInterval(tick,1000); tick();
    </script>
    """
    with st.sidebar:
        components.html(clock_html, height=290)

# --- 5. DATA FETCHING ---
@st.cache_data(ttl=60)
def get_dashboard_data(area, date):
    engine = get_db_connection()
    
    # 1. Cast a wider net (Fetch Yesterday, Today, and Tomorrow in UTC)
    target_date = pd.to_datetime(date)
    date_str = target_date.strftime('%Y-%m-%d') # --- FIXED: Create a text string ---
    
    yest = (target_date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    tom = (target_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    
    query = f"""
        SELECT datetime_utc, predicted_co2, market_price_dkk_kwh as price, recommendation_status 
        FROM ai_forecasts 
        WHERE price_area = '{area}' 
        AND DATE(datetime_utc) IN ('{yest}', '{date_str}', '{tom}')
        ORDER BY datetime_utc ASC
    """
    
    df = pd.read_sql(query, engine)
    
    if not df.empty:
        # 2. Convert the whole batch to Danish Time safely
        dt_col = pd.to_datetime(df['datetime_utc'])
        if dt_col.dt.tz is None:
            utc_time = dt_col.dt.tz_localize('UTC')
        else:
            utc_time = dt_col
            
        dk_time = utc_time.dt.tz_convert('Europe/Copenhagen')
        
        # 3. Create the format strings
        df['dk_date'] = dk_time.dt.strftime('%Y-%m-%d')
        df['Time'] = dk_time.dt.strftime('%H:00')
        
        # 4. Filter down strictly to the Danish day using the STRING
        df = df[df['dk_date'] == date_str].copy()
        
        c_map = {'BEST': '#10B981', 'CAUTION': '#F59E0B', 'AVOID': '#EF4444'}
        df['bar_color'] = df['recommendation_status'].map(c_map)
        
    # Reset index to ensure the clock logic loops from 0 to 23 correctly
    return df.reset_index(drop=True)

@st.cache_data(ttl=60)
def get_dates(area):
    engine = get_db_connection()
    return pd.read_sql(f"SELECT DISTINCT DATE(datetime_utc) as d FROM ai_forecasts WHERE price_area='{area}' ORDER BY d DESC LIMIT 14", engine)['d'].tolist()

# --- APP LAYOUT ---
st.set_page_config(page_title="Guardian", layout="wide")
apply_addictive_ui()

# Sidebar
st.sidebar.title("GRIDWISE")
area = st.sidebar.selectbox("Region", ["DK1", "DK2"])
dates = get_dates(area)
date = st.sidebar.selectbox("Timeline", dates) if dates else None

if date:
    df = get_dashboard_data(area, date)
    render_sidebar_clock(df)

    st.title(f"{area} Strategy | {date}")

    # --- KPI CARDS ---
    if not df.empty:
        best = df.loc[df['predicted_co2'].idxmin()]
        worst = df.loc[df['predicted_co2'].idxmax()]
        mean_co2 = df['predicted_co2'].mean()

        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Optimal Point</div><div class="kpi-value">{best["Time"]}</div><div class="kpi-sub" style="color:#10B981">{best["predicted_co2"]:.0f}g CO2</div></div>', unsafe_allow_html=True)
        with c2:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Peak Stress</div><div class="kpi-value">{worst["Time"]}</div><div class="kpi-sub" style="color:#EF4444">{worst["predicted_co2"]:.0f}g CO2</div></div>', unsafe_allow_html=True)
        with c3:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Grid Mean</div><div class="kpi-value">{mean_co2:.1f}g</div><div class="kpi-sub" style="color:#38BDF8">Intensity Baseline</div></div>', unsafe_allow_html=True)

        # --- CHART ---
        st.markdown("### Forecast Overview")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df['Time'], y=df['price'], marker_color=df['bar_color'], opacity=0.4))
        fig.add_trace(go.Scatter(x=df['Time'], y=df['predicted_co2'], mode='lines', line=dict(color='#38BDF8', width=4, shape='spline'), yaxis='y2'))
        
        # --- ADDED: Danish Timezone for the Chart Line ---
        dk_tz = pytz.timezone('Europe/Copenhagen')
        cur_h = datetime.now(dk_tz).strftime('%H:00')
        if cur_h in df['Time'].values:
            fig.add_vline(x=cur_h, line_width=2, line_dash="dot", line_color="rgba(255,255,255,0.4)")

        fig.update_layout(
            template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            yaxis=dict(showgrid=False, title=None), yaxis2=dict(overlaying='y', side='right', showgrid=False),
            margin=dict(l=0, r=0, t=0, b=0), height=380, showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)

        # --- 5. THE BOLD BOLD HIGHLIGHT TABLE ---
        st.markdown("### Hourly Breakdown")
        
        table_df = pd.DataFrame({
            "Time": df['Time'],
            "Price": df['price'].round(3),
            "CO2 (g)": df['predicted_co2'].round(1),
            "Status": df['recommendation_status'].map({'BEST': '🟢 BEST', 'CAUTION': '🟡 CAUTION', 'AVOID': '🔴 AVOID'})
        })

        def highlight_and_bold(row):
            # --- ADDED: Danish Timezone for the Glowing Row ---
            dk_tz = pytz.timezone('Europe/Copenhagen')
            cur_h = datetime.now(dk_tz).strftime('%H:00')
            if row['Time'] == cur_h:
                # The Active "Seductive" Row: Deep cyan background with glowing cyan text
                return ['background-color: rgba(0, 209, 255, 0.1); font-weight: 900; color: #00D1FF;'] * len(row)
            
            # The Base Rows: Transparent background with sleek slate-grey text
            return ['background-color: transparent; color: #94A3B8; font-weight: 500;'] * len(row)

        st.dataframe(
            table_df.style.apply(highlight_and_bold, axis=1),
            use_container_width=True, hide_index=True
        )
    else:
        st.warning("No data available for this date.")
