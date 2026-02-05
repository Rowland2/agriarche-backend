import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# --- UI CONFIG ---
st.set_page_config(page_title="Agriarche Intelligence Hub", layout="wide")

# --- CUSTOM BRANDING & LAYOUT (CSS) ---
st.markdown(f"""
    <style>
    /* Force Main Background to Pure White */
    .stApp {{
        background-color: #FFFFFF;
    }}
    
    /* Sidebar Background (Agriarche Gold) */
    [data-testid="stSidebar"] {{
        background-color: #F4B266;
    }}
    
    /* Sidebar Text Styles */
    [data-testid="stSidebar"] label, [data-testid="stSidebar"] p {{
        color: #2E7D32 !important;
        font-weight: bold !important;
    }}

    /* KPI Card Styling - Long, Rectangular, Green Borders */
    .kpi-container {{
        display: flex;
        flex-direction: row;
        justify-content: space-between;
        gap: 15px;
        width: 100%;
        margin-top: 40px;
        margin-bottom: 20px;
    }}
    .kpi-card {{
        background-color: #FFFFFF;
        border: 1px solid #2E7D32; /* Thin Forest Green Border */
        border-radius: 4px;
        padding: 20px;
        flex: 1;
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        box-shadow: 2px 2px 8px rgba(0,0,0,0.05);
    }}
    .kpi-label {{
        color: #444444;
        font-size: 14px;
        font-weight: 600;
        margin-bottom: 8px;
    }}
    .kpi-value {{
        color: #2E7D32;
        font-size: 30px;
        font-weight: 800;
    }}
    </style>
    """, unsafe_allow_html=True)

# --- API CONFIGURATION ---
BASE_URL = "https://agriarche-backend.onrender.com"
HEADERS = {"access_token": "Agriarche_Internal_Key_2026"}

# --- SIDEBAR FILTERS ---
st.sidebar.image("https://via.placeholder.com/150x50?text=Agriarche+Logo", use_container_width=True) 
st.sidebar.header("Market Filters")

commodity = st.sidebar.selectbox("Select Commodity", ["Maize White", "Soya Beans", "Rice Paddy", "Millet", "Sorghum Red", "Cowpea White", "Groundnut Gargaja", "Groundnut Kampala"])
market = st.sidebar.selectbox("Select Market", ["All Markets", "Biliri", "Dawanau", "Potiskum", "Giwa"])
month = st.sidebar.selectbox("Select Month", ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
price_choice = st.sidebar.radio("Display Price By:", ["Price per Kg", "Price per Bag"])

db_column = "price_per_kg" if price_choice == "Price per Kg" else "price_per_bag"

# --- MAIN CONTENT ---
st.markdown(f"<h1 style='color: #2E7D32; margin-bottom: 5px;'>Commodity Pricing Intelligence Dashboard</h1>", unsafe_allow_html=True)
st.markdown(f"<h3 style='color: #2E7D32; font-weight: normal; margin-top: 0;'>Kasuwa Internal Price Trend (per Kg): {commodity} in {month}</h3>", unsafe_allow_html=True)

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

        if chart_data and metrics:
            # --- 1. THE HARMONY CHART (TOP) ---
            df_plot = pd.DataFrame(chart_data)
            df_plot['start_time'] = pd.to_datetime(df_plot['start_time'])
            df_plot[db_column] = pd.to_numeric(df_plot[db_column])

            # Grouping by day for a smooth trend line
            df_daily = df_plot.groupby(df_plot['start_time'].dt.day)[db_column].mean().reset_index()
            df_daily.columns = ['Day', 'Price']

            fig = px.line(df_daily, x="Day", y="Price", markers=True, text=df_daily['Price'].round(0))

            # Style the line with Agriarche Gold (#F4B266)
            fig.update_traces(
                line_color="#F4B266", 
                line_width=4,
                marker=dict(size=10, color="#F4B266", line=dict(width=2, color='white')),
                textposition="top center"
            )

            # Bold Axes and white background
            fig.update_layout(
                plot_bgcolor="white",
                xaxis=dict(showline=True, linewidth=4, linecolor='black', title="<b>Day of Month</b>", gridcolor='#F0F0F0'),
                yaxis=dict(showline=True, linewidth=4, linecolor='black', title=f"<b>{price_choice} (₦)</b>", gridcolor='#F0F0F0'),
                height=500,
                margin=dict(t=30, b=30, l=50, r=50)
            )
            
            st.plotly_chart(fig, use_container_width=True)

            # --- 2. THE KPI CARDS (BOTTOM) ---
            st.markdown(f"""
                <div class="kpi-container">
                    <div class="kpi-card">
                        <div class="kpi-label">Avg Kasuwa internal price</div>
                        <div class="kpi-value">₦{metrics['avg']:,.0f}</div>
                    </div>
                    <div class="kpi-card">
                        <div class="kpi-label">Highest Kasuwa internal price</div>
                        <div class="kpi-value">₦{metrics['max']:,.0f}</div>
                    </div>
                    <div class="kpi-card">
                        <div class="kpi-label">Lowest Kasuwa internal price</div>
                        <div class="kpi-value">₦{metrics['min']:,.0f}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            # --- 3. MARKET INTELLIGENCE ---
            if intel_data.get("info"):
                st.info(f"**Market Intelligence:** {intel_data['info'].get('desc')}")
        else:
            st.warning(f"No pricing data found for {commodity} in {month}.")
    else:
        st.error(f"Backend Error: {response.status_code}")

except Exception as e:
    st.error(f"Connection Error: {e}")