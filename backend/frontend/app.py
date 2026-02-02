import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# UI CONFIG
st.set_page_config(page_title="Agriarche Dashboard", layout="wide")

# --- API CONFIGURATION ---
BASE_URL = "http://127.0.0.1:8000"
HEADERS = {"access_token": "Agriarche_Internal_Key_2026"} 

st.title("🌾 Agriarche Intelligence Hub")

# --- SIDEBAR (MATCHES SCREENSHOT) ---
st.sidebar.header("Market Filters")

commodity = st.sidebar.selectbox(
    "Select Commodity",
    ["Cowpea Brown", "Cowpea White", "Maize", "Soybeans", "Rice Paddy"]
)

market = st.sidebar.selectbox(
    "Select Kasuwa internal price Market",
    ["All Markets", "Dawanau", "Potiskum", "Giwa"]
)

month = st.sidebar.selectbox(
    "Select Month",
    ["January", "February", "March", "April", "May", "June",
     "July", "August", "September", "October", "November", "December"]
)

years = st.sidebar.multiselect(
    "Year",
    [2024, 2025, 2026],
    default=[2025, 2026]
)

# --- FETCH DATA ---
try:
    analysis_params = {
        "commodity": commodity,
        "month": month,
        "market": market,
        "years": ",".join(map(str, years))
    }

    response = requests.get(
        f"{BASE_URL}/analysis",
        params=analysis_params,
        headers=HEADERS
    )

    
    # 2. Fetch Intelligence Description
    intel_response = requests.get(f"{BASE_URL}/intelligence/{comm}", headers=HEADERS)
    intel_data = intel_response.json() if intel_response.status_code == 200 else {}

    if response.status_code == 200:
        # Get the full JSON package
        data_package = response.json()
        
        # Extract metrics and chart_data from the package
        metrics = data_package.get("metrics")
        chart_data = data_package.get("chart_data")

        if metrics and metrics.get('avg', 0) > 0:
            # Title Styling
            st.markdown(f"<h2 style='color: #1F7A3F; text-align: center;'>Kasuwa Internal Price Trend (per Kg): {comm} in {month}</h2>", unsafe_allow_html=True)

            # --- THE TREND CHART ---
            if chart_data:
                df_plot = pd.DataFrame(chart_data)
                df_plot['price_label'] = df_plot['price'].round(0)
                
                fig = px.line(
    df_plot,
    x="day",
    y="price",
    markers=True,
    text=df_plot['price'].round(0)
)

fig.update_traces(
    line_color="#E67E22",
    marker=dict(size=9, color="#E67E22"),
    textposition="top center"
)

fig.update_layout(
    plot_bgcolor="white",
    xaxis_title="Day of Month",
    yaxis_title="Price per Kg (₦)",
    xaxis=dict(dtick=1),
    yaxis=dict(showgrid=True, gridcolor="#F0F0F0"),
)

                
                # Styling to match your orange trendline request
                fig.update_traces(
                    line_color='#E67E22', 
                    marker=dict(size=10, color='#E67E22'), 
                    textposition="top center"
                )
                fig.update_layout(
                    plot_bgcolor='white', 
                    xaxis=dict(showgrid=True, gridcolor='#F0F0F0', dtick=1), 
                    yaxis=dict(showgrid=True, gridcolor='#F0F0F0')
                )
                st.plotly_chart(fig, use_container_width=True)

            # --- METRIC CARDS (Bottom Row) ---
            st.write("---")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(f"<div style='border-top: 5px solid #1F7A3F; padding:10px; background:#F9F9F9; text-align:center;'><b>Avg Price / Kg</b><br><h3>₦{metrics['avg']:,.2f}</h3></div>", unsafe_allow_html=True)
            with col2:
                st.markdown(f"<div style='border-top: 5px solid #1F7A3F; padding:10px; background:#F9F9F9; text-align:center;'><b>Max Price / Kg</b><br><h3>₦{metrics['max']:,.2f}</h3></div>", unsafe_allow_html=True)
            with col3:
                st.markdown(f"<div style='border-top: 5px solid #1F7A3F; padding:10px; background:#F9F9F9; text-align:center;'><b>Min Price / Kg</b><br><h3>₦{metrics['min']:,.2f}</h3></div>", unsafe_allow_html=True)

            # --- INTELLIGENCE INFO ---
            if intel_data.get("info"):
                st.write("")
                st.info(f"**Intelligence:** {intel_data['info'].get('desc')}")

        else:
            st.warning(f"No data available for {comm} in {month}.")
    else:
        st.error(f"Backend Error {response.status_code}: Could not fetch analysis data.")

except Exception as e:
    st.error(f"Could not connect to Backend API. Is it running? Error: {e}")