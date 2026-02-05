import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# --- UI CONFIG ---
st.set_page_config(page_title="Agriarche Intelligence Hub", layout="wide")

# --- CUSTOM BRANDING (CSS) ---
st.markdown(f"""
    <style>
    /* Sidebar Background Color (Agriarche Gold) */
    [data-testid="stSidebar"] {{
        background-color: #F4B266;
    }}
    /* Sidebar Text Color */
    [data-testid="stSidebar"] .stSelectbox label, [data-testid="stSidebar"] .stMultiSelect label, [data-testid="stSidebar"] p {{
        color: #1E1E1E !important;
        font-weight: bold;
    }}
    /* Metric Card Styling */
    [data-testid="stMetricLabel"] {{
        font-size: 1.2rem !important;
        font-weight: bold !important;
        color: #555555 !important;
    }}
    [data-testid="stMetricValue"] {{
        color: #2E7D32 !important;
    }}
    </style>
    """, unsafe_allow_html=True)

# --- API CONFIGURATION ---
BASE_URL = "https://agriarche-backend.onrender.com"
HEADERS = {"access_token": "Agriarche_Internal_Key_2026"}

st.title("🌾 Agriarche Intelligence Hub")

# --- SIDEBAR FILTERS ---
st.sidebar.image("https://via.placeholder.com/150x50?text=Agriarche+Logo", use_container_width=True) # Replace with your actual logo URL
st.sidebar.header("Market Filters")

commodity = st.sidebar.selectbox("Select Commodity", ["Maize White", "Soya Beans", "Rice Paddy", "Millet", "Sorghum Red", "Cowpea White", "Groundnut Gargaja", "Groundnut Kampala"])
market = st.sidebar.selectbox("Select Market", ["All Markets", "Biliri", "Dawanau", "Potiskum", "Giwa"])
month = st.sidebar.selectbox("Select Month", ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
price_choice = st.sidebar.radio("Display Price By:", ["Price per Kg", "Price per Bag"])

db_column = "price_per_kg" if price_choice == "Price per Kg" else "price_per_bag"

# --- FETCH & DISPLAY DATA ---
try:
    analysis_params = {"commodity": commodity, "month": month, "market": market}
    
    # Fetch Data
    response = requests.get(f"{BASE_URL}/analysis", params=analysis_params, headers=HEADERS)
    intel_response = requests.get(f"{BASE_URL}/intelligence/{commodity}", headers=HEADERS)
    intel_data = intel_response.json() if intel_response.status_code == 200 else {}

    if response.status_code == 200:
        data_package = response.json()
        metrics = data_package.get("metrics")
        chart_data = data_package.get("chart_data")

        if metrics and metrics.get('avg', 0) > 0:
            # --- 1. KPI CARDS ---
            st.markdown("### 📊 Market Overview")
            col1, col2, col3 = st.columns(3)
            col1.metric("Average Price", f"₦{metrics['avg']:,.2f}")
            col2.metric("Highest Price", f"₦{metrics['max']:,.2f}")
            col3.metric("Lowest Price", f"₦{metrics['min']:,.2f}")

            # --- 2. THE HARMONY CHART ---
            if chart_data:
                df_plot = pd.DataFrame(chart_data)
                df_plot['start_time'] = pd.to_datetime(df_plot['start_time'])
                df_plot[db_column] = pd.to_numeric(df_plot[db_column])

                # AGGREGATION: Averages multiple markets per day for a smooth line
                df_daily = df_plot.groupby(df_plot['start_time'].dt.date)[db_column].mean().reset_index()
                df_daily.columns = ['date', 'price']

                fig = px.line(df_daily, x="date", y="price", markers=True, text=df_daily['price'].round(0))

                fig.update_traces(
                    line_color="#2E7D32", # Agriarche Forest Green
                    line_width=4,
                    marker=dict(size=10, color="#F4B266", line=dict(width=2, color='white')), # Agriarche Gold markers
                    textposition="top center"
                )

                fig.update_layout(
                    plot_bgcolor="white",
                    xaxis=dict(showline=True, linewidth=3, linecolor='black', title="<b>Date</b>"),
                    yaxis=dict(showline=True, linewidth=3, linecolor='black', title=f"<b>{price_choice} (₦)</b>", gridcolor='#EEEEEE'),
                    height=500,
                    margin=dict(t=20)
                )
                
                st.plotly_chart(fig, use_container_width=True)

            if intel_data.get("info"):
                st.info(f"**Market Intelligence:** {intel_data['info'].get('desc')}")
        else:
            st.warning(f"No pricing data found for {commodity} in {month}.")
    else:
        st.error(f"Backend Error: {response.status_code}")

except Exception as e:
    st.error(f"Connection Error: {e}")