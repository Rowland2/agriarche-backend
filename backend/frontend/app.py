import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# --- UI CONFIG ---
st.set_page_config(page_title="Agriarche Dashboard", layout="wide")

# --- API CONFIGURATION ---
# Base URL of your live Render backend
BASE_URL = "https://agriarche-backend.onrender.com"
HEADERS = {"access_token": "Agriarche_Internal_Key_2026"}

st.title("🌾 Agriarche Intelligence Hub")

# --- SIDEBAR FILTERS ---
st.sidebar.header("Market Filters")

commodity = st.sidebar.selectbox(
    "Select Commodity",
    ["Maize White", "Soya Beans", "Rice Paddy", "Millet", "Sorghum Red", "Cowpea White", "Groundnut Gargaja", "Groundnut Kampala"]
)

market = st.sidebar.selectbox(
    "Select Market",
    ["All Markets", "Biliri", "Dawanau", "Potiskum", "Giwa"]
)

month = st.sidebar.selectbox(
    "Select Month",
    ["January", "February", "March", "April", "May", "June",
     "July", "August", "September", "October", "November", "December"]
)

# Year selection (Note: Backend currently handles filter by commodity/month/market)
years = st.sidebar.multiselect("Year", [2024, 2025, 2026], default=[2026])

price_choice = st.sidebar.radio("Display Price By:", ["Price per Kg", "Price per Bag"])

# Mapping UI selection to the exact snake_case columns in your Neon DB
db_column = "price_per_kg" if price_choice == "Price per Kg" else "price_per_bag"

# --- FETCH & DISPLAY DATA ---
try:
    # Prepare parameters for the /analysis endpoint
    analysis_params = {
        "commodity": commodity,
        "month": month,
        "market": market
    }

    # 1. Fetch Analysis & Chart Data
    response = requests.get(f"{BASE_URL}/analysis", params=analysis_params, headers=HEADERS)
    
    # 2. Fetch Intelligence Description (If route exists)
    intel_response = requests.get(f"{BASE_URL}/intelligence/{commodity}", headers=HEADERS)
    intel_data = intel_response.json() if intel_response.status_code == 200 else {}

    if response.status_code == 200:
        data_package = response.json()
        metrics = data_package.get("metrics")
        chart_data = data_package.get("chart_data")

        # Check if we have valid data (Avg > 0)
        if metrics and metrics.get('avg', 0) > 0:
            st.markdown(f"<h2 style='color: #1F7A3F; text-align: center;'>Kasuwa Internal Price Trend: {commodity} ({price_choice})</h2>", unsafe_allow_html=True)

            # --- THE TREND CHART ---
            if chart_data:
                df_plot = pd.DataFrame(chart_data)
                
                # Convert price columns to numeric just in case they arrived as strings
                df_plot[db_column] = pd.to_numeric(df_plot[db_column], errors='coerce')
                
                fig = px.line(
                    df_plot,
                    x="start_time",  # Match snake_case from Backend
                    y=db_column,     # Match snake_case from Backend
                    markers=True,
                    text=df_plot[db_column].round(2),
                    title=f"{commodity} Price Trend"
                )

                fig.update_traces(
                    line_color="#E67E22",
                    marker=dict(size=10, color="#E67E22"),
                    textposition="top center"
                )

                fig.update_layout(
                    plot_bgcolor="white",
                    xaxis_title="Date",
                    yaxis_title=f"{price_choice} (₦)",
                    height=450
                )
                
                st.plotly_chart(fig, use_container_width=True)

            # --- METRIC CARDS ---
            st.write("---")
            col1, col2, col3 = st.columns(3)
            card_style = "border-top: 5px solid #1F7A3F; border-radius: 5px; padding:15px; background:#F9F9F9; text-align:center; box-shadow: 2px 2px 5px rgba(0,0,0,0.05);"
            
            with col1:
                st.markdown(f"<div style='{card_style}'><b>Avg Price</b><br><h2 style='color: #1F7A3F;'>₦{metrics['avg']:,.2f}</h2></div>", unsafe_allow_html=True)
            with col2:
                st.markdown(f"<div style='{card_style}'><b>Highest Price</b><br><h2 style='color: #1F7A3F;'>₦{metrics['max']:,.2f}</h2></div>", unsafe_allow_html=True)
            with col3:
                st.markdown(f"<div style='{card_style}'><b>Lowest Price</b><br><h2 style='color: #1F7A3F;'>₦{metrics['min']:,.2f}</h2></div>", unsafe_allow_html=True)

            # Display intelligence if available
            if intel_data.get("info"):
                st.info(f"**Intelligence:** {intel_data['info'].get('desc')}")
        else:
            st.warning(f"No pricing data found for {commodity} in {month}.")
    else:
        st.error(f"Backend Error {response.status_code}: {response.text}")

except Exception as e:
    st.error(f"Could not connect to Backend API. Error: {e}")