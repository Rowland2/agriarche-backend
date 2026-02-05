import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# --- 1. BRANDING CONFIGURATION ---
PRIMARY_COLOR = "#1F7A3F"  # Forest Green
ACCENT_COLOR = "#F4B266"   # Agriarche Gold
BG_COLOR = "#F5F7FA"       # Light Grey Background

st.set_page_config(page_title="Agriarche Intelligence Hub", layout="wide")

# --- 2. THE SPECIFIED FORMATTING (CSS) ---
st.markdown(f"""
    <style>
        header {{ visibility: hidden; }}
        .stApp {{ background-color: {BG_COLOR}; }}
        section[data-testid="stSidebar"] {{ background-color: {ACCENT_COLOR} !important; }}
        section[data-testid="stSidebar"] div[data-baseweb="select"] > div {{
            background-color: #FFFFFF !important;
            color: #000000 !important;
        }}
        div[role="listbox"] ul li {{ color: #000000 !important; }}
        section[data-testid="stSidebar"] .stMarkdown p, 
        section[data-testid="stSidebar"] label {{
            color: #000000 !important;
            font-weight: bold !important;
        }}
        h1, h2, h3 {{ color: {PRIMARY_COLOR} !important; }}
        
        /* KPI Card Styling - CURVED & RECTANGULAR */
        .metric-container {{
            display: flex;
            justify-content: space-between;
            gap: 15px;
            margin-top: 30px;
            margin-bottom: 20px;
        }}
        .metric-card {{
            background-color: white;
            padding: 20px;
            border-radius: 15px; /* Curved corners matching Screenshot 1128 */
            border-left: 8px solid {PRIMARY_COLOR}; /* Stronger left accent */
            box-shadow: 2px 4px 10px rgba(0,0,0,0.05);
            width: 100%;
            text-align: left; /* Left aligned labels per professional dashboards */
        }}
        .metric-label {{ font-size: 14px; color: #555; font-weight: bold; margin-bottom: 5px; }}
        .metric-value {{ font-size: 28px; color: {PRIMARY_COLOR}; font-weight: 800; }}

        /* Strategy Cards */
        .strategy-card {{
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            color: white;
            margin-bottom: 20px;
        }}
        .best-buy {{ background-color: #2E7D32; border-bottom: 5px solid #1B5E20; }}
        .worst-buy {{ background-color: #C62828; border-bottom: 5px solid #8E0000; }}

        /* AI Advisor Styling */
        .advisor-container {{
            background-color: #FFFFFF;
            padding: 20px;
            border-radius: 10px;
            border-left: 5px solid {PRIMARY_COLOR};
            margin-bottom: 25px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        }}
        .stAlert p {{ color: #000000 !important; }}
    </style>
""", unsafe_allow_html=True)

# --- 3. API CONFIGURATION ---
BASE_URL = "https://agriarche-backend.onrender.com"
HEADERS = {"access_token": "Agriarche_Internal_Key_2026"}

# --- 4. SIDEBAR FILTERS ---
st.sidebar.title("Market Filters")
commodity = st.sidebar.selectbox("Select Commodity", ["Maize White", "Soya Beans", "Rice Paddy", "Millet", "Sorghum Red", "Cowpea White"])
market = st.sidebar.selectbox("Select Market", ["All Markets", "Biliri", "Dawanau", "Potiskum", "Giwa"])
month = st.sidebar.selectbox("Select Month", ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
price_choice = st.sidebar.radio("Display Price By:", ["Price per Kg", "Price per Bag"])

db_column = "price_per_kg" if price_choice == "Price per Kg" else "price_per_bag"

# --- 5. MAIN CONTENT ---
st.title("Commodity Pricing Intelligence Dashboard")
st.subheader(f"Kasuwa Internal Price Trend: {commodity} in {month}")

try:
    response = requests.get(f"{BASE_URL}/analysis", params={"commodity": commodity, "month": month, "market": market}, headers=HEADERS)
    
    if response.status_code == 200:
        data = response.json()
        metrics = data.get("metrics")
        chart_data = data.get("chart_data")

        if chart_data:
            # --- THE CHART (TOP) ---
            df = pd.DataFrame(chart_data)
            df['start_time'] = pd.to_datetime(df['start_time'])
            df_daily = df.groupby(df['start_time'].dt.day)[db_column].mean().reset_index()
            df_daily.columns = ['Day', 'Price']

            fig = px.line(df_daily, x="Day", y="Price", markers=True, text=df_daily['Price'].round(0))
            fig.update_traces(line_color=ACCENT_COLOR, line_width=4, marker=dict(size=10, color=ACCENT_COLOR, line=dict(width=2, color='white')))
            fig.update_layout(
                plot_bgcolor="white",
                xaxis=dict(showline=True, linewidth=4, linecolor='black', title="<b>Day of Month</b>"),
                yaxis=dict(showline=True, linewidth=4, linecolor='black', title=f"<b>{price_choice} (₦)</b>"),
                height=450
            )
            st.plotly_chart(fig, use_container_width=True)

            # --- THE KPI CARDS (BOTTOM - CURVED & LONG) ---
            st.markdown(f"""
                <div class="metric-container">
                    <div class="metric-card">
                        <div class="metric-label">Avg Kasuwa internal price</div>
                        <div class="metric-value">₦{metrics['avg']:,.0f}</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Highest Kasuwa internal price</div>
                        <div class="metric-value">₦{metrics['max']:,.0f}</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Lowest Kasuwa internal price</div>
                        <div class="metric-value">₦{metrics['min']:,.0f}</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)

except Exception as e:
    st.error(f"UI Loading Error: {e}")