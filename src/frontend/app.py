import sys
import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import pytz
from sqlalchemy import text

# --- PATH FIX ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.database.connection import get_db_connection


# ============================================================
# CONFIG
# ============================================================

PRICE_AREA = "DK1"
CPH_TZ = "Europe/Copenhagen"


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Greenhour Guardian",
    page_icon="⚡",
    layout="wide",
)


# ============================================================
# UI STYLE
# ============================================================

def apply_ui_style():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

        .stApp {
            background: linear-gradient(180deg, #08111f 0%, #05070a 100%);
            color: #F8FAFC;
            font-family: 'Inter', sans-serif;
        }

        h1, h2, h3 {
            color: #F8FAFC !important;
            font-weight: 800 !important;
            letter-spacing: -0.03em;
        }

        .subtitle {
            color: #94A3B8;
            font-size: 1.05rem;
            margin-top: -10px;
            margin-bottom: 24px;
        }

        .kpi-card {
            background: rgba(15, 23, 42, 0.72);
            border: 1px solid rgba(148, 163, 184, 0.18);
            border-radius: 22px;
            padding: 24px 22px;
            min-height: 145px;
            box-shadow: 0 18px 50px rgba(0,0,0,0.25);
        }

        .kpi-label {
            font-size: 0.72rem;
            text-transform: uppercase;
            letter-spacing: 0.16em;
            color: #94A3B8;
            font-weight: 700;
            margin-bottom: 12px;
        }

        .kpi-value {
            font-size: 2.25rem;
            font-weight: 800;
            line-height: 1.05;
            color: #F8FAFC;
        }

        .kpi-sub {
            font-size: 0.9rem;
            margin-top: 12px;
            color: #CBD5E1;
        }

        .explain-box {
            background: rgba(15, 23, 42, 0.62);
            border: 1px solid rgba(148, 163, 184, 0.16);
            border-radius: 18px;
            padding: 18px 20px;
            color: #CBD5E1;
            line-height: 1.6;
        }

        section[data-testid="stSidebar"] {
            background: #05070A !important;
            border-right: 1px solid rgba(148, 163, 184, 0.12);
        }

        div[data-testid="stDataFrame"] {
            border-radius: 18px;
            overflow: hidden;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def status_color(status):
    return {
        "BEST": "#10B981",
        "CAUTION": "#F59E0B",
        "AVOID": "#EF4444",
    }.get(status, "#94A3B8")


def status_emoji(status):
    return {
        "BEST": "🟢 BEST",
        "CAUTION": "🟡 CAUTION",
        "AVOID": "🔴 AVOID",
    }.get(status, status)


# ============================================================
# DATA LOADING
# ============================================================

@st.cache_data(ttl=120)
def get_available_dates():
    engine = get_db_connection()

    query = text("""
        SELECT DISTINCT
            (datetime_utc AT TIME ZONE 'Europe/Copenhagen')::date AS date_cph
        FROM co2_aware_price_signals
        WHERE price_area = :price_area
        ORDER BY date_cph DESC
        LIMIT 14;
    """)

    df = pd.read_sql(
        query,
        engine,
        params={"price_area": PRICE_AREA},
    )

    if df.empty:
        return []

    return df["date_cph"].tolist()


@st.cache_data(ttl=120)
def get_dashboard_data(selected_date):
    engine = get_db_connection()

    target_start_cph = pd.Timestamp(str(selected_date), tz=CPH_TZ)
    target_end_cph = target_start_cph + pd.Timedelta(days=1)

    start_utc = target_start_cph.tz_convert("UTC")
    end_utc = target_end_cph.tz_convert("UTC")

    query = text("""
        SELECT
            datetime_utc,
            price_area,
            spot_price_dkk_kwh,
            predicted_co2_g_kwh,
            normalized_price,
            normalized_co2,
            raw_co2_aware_signal,
            recommendation_status,
            should_charge,
            is_peak_hour,
            model_name,
            model_version,
            price_weight,
            co2_weight
        FROM co2_aware_price_signals
        WHERE price_area = :price_area
          AND datetime_utc >= :start_utc
          AND datetime_utc < :end_utc
        ORDER BY datetime_utc ASC;
    """)

    df = pd.read_sql(
        query,
        engine,
        params={
            "price_area": PRICE_AREA,
            "start_utc": start_utc.to_pydatetime(),
            "end_utc": end_utc.to_pydatetime(),
        },
    )

    if df.empty:
        return df

    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df["datetime_cph"] = df["datetime_utc"].dt.tz_convert(CPH_TZ)
    df["date_cph"] = df["datetime_cph"].dt.strftime("%Y-%m-%d")
    df["time_cph"] = df["datetime_cph"].dt.strftime("%H:00")

    df["bar_color"] = df["recommendation_status"].map(status_color)
    df["status_label"] = df["recommendation_status"].map(status_emoji)

    return df.reset_index(drop=True)


# ============================================================
# UI COMPONENTS
# ============================================================

def render_kpi_card(label, value, sub, color="#F8FAFC"):
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value" style="color:{color};">{value}</div>
            <div class="kpi-sub">{sub}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_main_comparison_chart(df, chart_mode):
    fig = go.Figure()

    if chart_mode == "Normalized comparison":
        fig.add_trace(
            go.Scatter(
                x=df["time_cph"],
                y=df["normalized_price"],
                mode="lines+markers",
                name="Normalized price",
                line=dict(width=3),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["time_cph"],
                y=df["normalized_co2"],
                mode="lines+markers",
                name="Normalized predicted CO₂",
                line=dict(width=3),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["time_cph"],
                y=df["raw_co2_aware_signal"],
                mode="lines+markers",
                name="CO₂-aware price signal",
                line=dict(width=5),
            )
        )

        fig.update_layout(
            yaxis=dict(
                title="Normalized value / signal",
                range=[-0.05, 1.05],
                gridcolor="rgba(148,163,184,0.15)",
            )
        )

    else:
        fig.add_trace(
            go.Bar(
                x=df["time_cph"],
                y=df["spot_price_dkk_kwh"],
                name="Original price DKK/kWh",
                marker_color=df["bar_color"],
                opacity=0.55,
                yaxis="y",
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df["time_cph"],
                y=df["predicted_co2_g_kwh"],
                mode="lines+markers",
                name="Original predicted CO₂ g/kWh",
                line=dict(width=4),
                yaxis="y2",
            )
        )

        fig.update_layout(
            yaxis=dict(
                title="Price DKK/kWh",
                gridcolor="rgba(148,163,184,0.15)",
            ),
            yaxis2=dict(
                title="Predicted CO₂ g/kWh",
                overlaying="y",
                side="right",
                gridcolor="rgba(148,163,184,0.05)",
            ),
        )

    # Current hour line if selected date is today in Copenhagen
    cph_now = datetime.now(pytz.timezone(CPH_TZ))
    current_date = cph_now.strftime("%Y-%m-%d")
    selected_date = df["date_cph"].iloc[0]

    if selected_date == current_date:
        current_hour = cph_now.strftime("%H:00")
        if current_hour in df["time_cph"].values:
            fig.add_vline(
                x=current_hour,
                line_width=2,
                line_dash="dot",
                line_color="rgba(255,255,255,0.55)",
            )

    fig.update_layout(
        template="plotly_dark",
        height=430,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        xaxis=dict(
            title="Hour in Copenhagen time",
            gridcolor="rgba(148,163,184,0.08)",
        ),
    )

    st.plotly_chart(fig, use_container_width=True)


def render_status_bar_chart(df):
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df["time_cph"],
            y=df["raw_co2_aware_signal"],
            marker_color=df["bar_color"],
            text=df["recommendation_status"],
            textposition="outside",
            name="Recommendation",
        )
    )

    fig.update_layout(
        template="plotly_dark",
        height=360,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=30, b=10),
        showlegend=False,
        yaxis=dict(
            title="Raw CO₂-aware price signal",
            range=[0, max(1.0, df["raw_co2_aware_signal"].max() + 0.1)],
            gridcolor="rgba(148,163,184,0.15)",
        ),
        xaxis=dict(
            title="Hour in Copenhagen time",
            gridcolor="rgba(148,163,184,0.08)",
        ),
    )

    st.plotly_chart(fig, use_container_width=True)


def render_hourly_table(df):
    table_df = pd.DataFrame(
        {
            "Time": df["time_cph"],
            "Price DKK/kWh": df["spot_price_dkk_kwh"].round(3),
            "Predicted CO₂ g/kWh": df["predicted_co2_g_kwh"].round(1),
            "Normalized price": df["normalized_price"].round(3),
            "Normalized CO₂": df["normalized_co2"].round(3),
            "CO₂-aware signal": df["raw_co2_aware_signal"].round(3),
            "Status": df["status_label"],
            "Peak hour": df["is_peak_hour"].map(lambda x: "Yes" if x else "No"),
        }
    )

    def style_rows(row):
        status = row["Status"]

        if "AVOID" in status:
            return [
                "background-color: rgba(239,68,68,0.12); color: #FCA5A5; font-weight: 700;"
            ] * len(row)

        if "BEST" in status:
            return [
                "background-color: rgba(16,185,129,0.12); color: #86EFAC; font-weight: 700;"
            ] * len(row)

        return [
            "background-color: rgba(245,158,11,0.08); color: #FCD34D; font-weight: 600;"
        ] * len(row)

    st.dataframe(
        table_df.style.apply(style_rows, axis=1),
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# APP
# ============================================================

apply_ui_style()

st.sidebar.title("Greenhour Guardian")
st.sidebar.caption("DK1 EV charging decision-support signal")

dates = get_available_dates()

if not dates:
    st.warning(
        "No data found in co2_aware_price_signals. "
        "Run predict_job.py and recommendation_job.py first."
    )
    st.stop()

selected_date = st.sidebar.selectbox(
    "Select Danish day",
    dates,
    index=0,
)

chart_mode = st.sidebar.radio(
    "Chart view",
    ["Normalized comparison", "Original price & CO₂"],
    index=0,
)

df = get_dashboard_data(selected_date)

if df.empty:
    st.warning("No signal data found for the selected Danish day.")
    st.stop()


# ============================================================
# HEADER
# ============================================================

st.title("Greenhour Guardian")
st.markdown(
    f"""
    <div class="subtitle">
    DK1 CO₂-aware EV charging signal for {selected_date}. 
    The dashboard compares DK1 day-ahead price, predicted CO₂ intensity, and the final CO₂-aware price signal.
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="explain-box">
    <b>How to read this dashboard:</b><br>
    The CO₂-aware price signal combines normalized DK1 day-ahead electricity price and normalized predicted CO₂ intensity.
    Lower values are more favourable for EV charging. Higher values are less favourable.
    The signal is not a real DKK/kWh tariff; it is a decision-support index.
    </div>
    """,
    unsafe_allow_html=True,
)

st.write("")


# ============================================================
# KPI CARDS
# ============================================================

avoid_row = df.loc[df["raw_co2_aware_signal"].idxmax()]
best_row = df.loc[df["raw_co2_aware_signal"].idxmin()]

avg_price = df["spot_price_dkk_kwh"].mean()
avg_co2 = df["predicted_co2_g_kwh"].mean()
avg_signal = df["raw_co2_aware_signal"].mean()

c1, c2, c3, c4 = st.columns(4)

with c1:
    render_kpi_card(
        label="Avoid hour",
        value=avoid_row["time_cph"],
        sub=(
            f"Signal {avoid_row['raw_co2_aware_signal']:.3f} | "
            f"{avoid_row['predicted_co2_g_kwh']:.0f} gCO₂/kWh"
        ),
        color="#EF4444",
    )

with c2:
    render_kpi_card(
        label="Average price",
        value=f"{avg_price:.3f}",
        sub="DKK/kWh across selected day",
        color="#38BDF8",
    )

with c3:
    render_kpi_card(
        label="Average CO₂",
        value=f"{avg_co2:.0f}g",
        sub="Predicted gCO₂/kWh",
        color="#F59E0B",
    )

with c4:
    render_kpi_card(
        label="Best hour",
        value=best_row["time_cph"],
        sub=(
            f"Signal {best_row['raw_co2_aware_signal']:.3f} | "
            f"{best_row['predicted_co2_g_kwh']:.0f} gCO₂/kWh"
        ),
        color="#10B981",
    )

st.write("")


# ============================================================
# CHARTS
# ============================================================

if chart_mode == "Normalized comparison":
    st.subheader("Normalized price vs predicted CO₂ vs CO₂-aware signal")
    st.caption(
        "All three lines are shown on a 0–1 scale so they can be compared fairly."
    )
else:
    st.subheader("Original DK1 price vs predicted CO₂")
    st.caption(
        "This view shows the real values: price in DKK/kWh and predicted CO₂ in gCO₂/kWh."
    )

render_main_comparison_chart(df, chart_mode)

st.subheader("Hourly recommendation from CO₂-aware signal")
st.caption(
    "Lowest 25% of signal values are BEST, middle 50% are CAUTION, and highest 25% are AVOID."
)
render_status_bar_chart(df)


# ============================================================
# SUMMARY COUNTS
# ============================================================

st.subheader("Daily recommendation summary")

count_df = (
    df["recommendation_status"]
    .value_counts()
    .reindex(["AVOID", "CAUTION", "BEST"])
    .fillna(0)
    .astype(int)
)

s1, s2, s3 = st.columns(3)

with s1:
    render_kpi_card(
        label="Avoid hours",
        value=str(count_df["AVOID"]),
        sub="Less favourable charging periods",
        color="#EF4444",
    )

with s2:
    render_kpi_card(
        label="Caution hours",
        value=str(count_df["CAUTION"]),
        sub="Moderate or mixed periods",
        color="#F59E0B",
    )

with s3:
    render_kpi_card(
        label="Best hours",
        value=str(count_df["BEST"]),
        sub="More favourable charging periods",
        color="#10B981",
    )


# ============================================================
# TABLE
# ============================================================

st.subheader("Hourly breakdown")
render_hourly_table(df)


# ============================================================
# FOOTER
# ============================================================

model_version = df["model_version"].iloc[0] if "model_version" in df.columns else "N/A"
price_weight = df["price_weight"].iloc[0] if "price_weight" in df.columns else 0.5
co2_weight = df["co2_weight"].iloc[0] if "co2_weight" in df.columns else 0.5

st.markdown(
    f"""
    <div class="explain-box">
    <b>Model version:</b> {model_version}<br>
    <b>Signal formula:</b> raw_co2_aware_signal = {price_weight:.1f} × normalized_price + {co2_weight:.1f} × normalized_CO₂<br>
    <b>Area:</b> DK1 | <b>Time zone shown:</b> Europe/Copenhagen
    </div>
    """,
    unsafe_allow_html=True,
)