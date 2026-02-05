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
        section[data-testid="stSidebar"] .stMarkdown p, 
        section[data-testid="stSidebar"] label {{
            color: #000000 !important;
            font-weight: bold !important;
        }}
        h1, h2, h3 {{ color: {PRIMARY_COLOR} !important; }}
        
        /* Curved KPI Card Styling */
        .metric-container {{
            display: flex;
            justify-content: space-between;
            gap: 15px;
            margin-top: 30px;
        }}
        .metric-card {{
            background-color: white;
            padding: 20px;
            border-radius: 15px;
            border-left: 8px solid {PRIMARY_COLOR};
            box-shadow: 2px 4px 10px rgba(0,0,0,0.05);
            width: 100%;
        }}
        .metric-label {{ font-size: 14px; color: #555; font-weight: bold; }}
        .metric-value {{ font-size: 28px; color: {PRIMARY_COLOR}; font-weight: 800; }}
    </style>
""", unsafe_allow_html=True)

# --- 3. API CONFIG ---
BASE_URL = "https://agriarche-backend.onrender.com"
HEADERS = {"access_token": "Agriarche_Internal_Key_2026"}

# --- 4. SIDEBAR ---
st.sidebar.title("Market Filters")
commodity = st.sidebar.selectbox("Select Commodity", ["Maize White", "Soya Beans", "Rice Paddy", "Millet", "Sorghum Red", "Cowpea White"])
market = st.sidebar.selectbox("Select Market", ["All Markets", "Biliri", "Dawanau", "Potiskum", "Giwa"])
month = st.sidebar.selectbox("Select Month", ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
price_choice = st.sidebar.radio("Display Price By:", ["Price per Kg", "Price per Bag"])

target_col = "price_per_kg" if price_choice == "Price per Kg" else "price_per_bag"

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
            df = pd.DataFrame(chart_data)
            df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
            df['start_time'] = pd.to_datetime(df['start_time'])
            
            # Prepare data for grouping
            df['day'] = df['start_time'].dt.day
            df['year'] = df['start_time'].dt.year.astype(str)
            
            dfc_grouped = df.groupby(['day', 'year'])[target_col].mean().reset_index()

            # --- THE NEW CHART FORMAT ---
            fig = px.line(dfc_grouped, x="day", y=target_col, color="year", markers=True,
                          text=dfc_grouped[target_col].apply(lambda x: f"<b>{x:,.0f}</b>"),
                          color_discrete_map={"2024": PRIMARY_COLOR, "2025": ACCENT_COLOR, "2026": "#E67E22"},
                          labels={"day": "Day of Month", target_col: f"{price_choice} (₦)"})
            
            fig.update_traces(textposition="top center")
            fig.update_layout(
                plot_bgcolor="white", paper_bgcolor="white", 
                font=dict(color="black", family="Arial Black"),
                xaxis=dict(
                    title=dict(text="<b>Day of Month</b>", font=dict(size=16, color="black")),
                    tickfont=dict(size=14, color="black", family="Arial Black"), 
                    showline=True, linecolor="black", linewidth=3, gridcolor="#eeeeee"
                ),
                yaxis=dict(
                    title=dict(text=f"<b>{price_choice} (₦)</b>", font=dict(size=16, color="black")),
                    tickfont=dict(size=14, color="black", family="Arial Black"), 
                    showline=True, linecolor="black", linewidth=3, gridcolor="#eeeeee"
                )
            )
            st.plotly_chart(fig, use_container_width=True)

            # --- CURVED KPI CARDS ---
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