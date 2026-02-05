import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import time  # New import for cache busting

# --- 1. BRANDING CONFIGURATION ---
PRIMARY_COLOR = "#1F7A3F"  # Forest Green
ACCENT_COLOR = "#F4B266"   # Agriarche Gold
BG_COLOR = "#F5F7FA"       # Light Grey Background

# --- COMMODITY INTELLIGENCE DATA ---
COMMODITY_INFO = {
    "Soya Beans": {"desc": "A raw leguminous crop used for oil and feed.", "markets": "Mubi, Giwa, and Kumo", "abundance": "Nov, Dec, and April", "note": "A key industrial driver for the poultry and vegetable oil sectors."},
    "Cowpea Brown": {"desc": "Protein-rich legume popular in local diets.", "markets": "Dawanau and Potiskum", "abundance": "Oct through Jan", "note": "Supply depends on Northern storage."},
    "Cowpea White": {"desc": "Staple bean variety used for commercial flour.", "markets": "Dawanau and Bodija", "abundance": "Oct and Nov", "note": "High demand in South drives prices."},
    "Honey beans": {"desc": "Premium sweet brown beans (Oloyin).", "markets": "Oyingbo and Dawanau", "abundance": "Oct to Dec", "note": "Often carries a price premium."},
    "Maize White": {"desc": "Primary cereal crop for food and industry.", "markets": "Giwa, Makarfi, and Funtua", "abundance": "Sept to Nov", "note": "Correlates strongly with Sorghum trends."},
    "Rice Paddy": {"desc": "Raw rice before milling/processing.", "markets": "Argungu and Kano", "abundance": "Nov and Dec", "note": "Foundations for processed rice pricing."},
    "Rice processed": {"desc": "Milled and polished local rice.", "markets": "Kano, Lagos, and Onitsha", "abundance": "Year-round", "note": "Price fluctuates with fuel/milling costs."},
    "Sorghum Red": {"desc": "Drought-resistant grain staple.", "markets": "Dawanau and Gombe", "abundance": "Dec and Jan", "note": "Market substitute for Maize."},
    "Millet": {"desc": "Fast-growing cereal for the lean season.", "markets": "Dawanau and Potiskum", "abundance": "Sept and Oct", "note": "First harvest after rainy season."},
    "Groundnut gargaja": {"desc": "Local peanut variety for oil extraction.", "markets": "Dawanau and Gombe", "abundance": "Oct and Nov", "note": "Sahel region specialty."},
    "Groundnut kampala": {"desc": "Large, premium roasting groundnuts.", "markets": "Kano and Dawanau", "abundance": "Oct and Nov", "note": "Higher oil content than Gargaja."}
}

def format_commodity_name(name):
    parts = name.split()
    colors = ["white", "brown", "red", "yellow", "black"]
    if len(parts) > 1 and parts[-1].lower() in colors:
        return f"{parts[-1].capitalize()} {' '.join(parts[:-1])}"
    return name.capitalize()

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
        .advisor-container {{
            background-color: #FFFFFF; padding: 20px; border-radius: 10px;
            border-left: 5px solid {ACCENT_COLOR}; margin-top: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }}
        .metric-container {{ display: flex; justify-content: space-between; gap: 15px; margin-top: 30px; }}
        .metric-card {{
            background-color: white; padding: 20px; border-radius: 15px;
            border-left: 8px solid {PRIMARY_COLOR}; box-shadow: 2px 4px 10px rgba(0,0,0,0.05); width: 100%;
        }}
        .metric-label {{ font-size: 14px; color: #555; font-weight: bold; }}
        .metric-value {{ font-size: 28px; color: {PRIMARY_COLOR}; font-weight: 800; }}
    </style>
""", unsafe_allow_html=True)

# --- 3. API CONFIG ---
BASE_URL = "https://agriarche-backend.onrender.com"
HEADERS = {"access_token": "Agriarche_Internal_Key_2026"}

# --- 4. SIDEBAR (DYNAMIC & LIVE) ---
st.sidebar.title("Market Filters")

@st.cache_data(ttl=5) 
def get_dynamic_filters():
    try:
        # Fetching data to extract every unique market in the DB
        res = requests.get(f"{BASE_URL}/analysis", headers=HEADERS)
        if res.status_code == 200:
            raw_data = res.json().get("chart_data", [])
            if raw_data:
                df_full = pd.DataFrame(raw_data)
                
                # Extract all unique commodities
                db_comms = sorted(df_full['commodity'].dropna().unique().tolist())
                
                # Extract EVERY unique market (using set and strip for names like Pambegua)
                db_mkts = sorted(list(set([str(m).strip() for m in df_full['market'] if m])))
                
                return db_comms, db_mkts
    except Exception as e:
        pass
    
    return list(COMMODITY_INFO.keys()), ["Biliri", "Potiskum", "Giwa", "Kumo"]

# 1. Get the lists
all_comms, all_mkts = get_dynamic_filters()

# 2. Sidebar Selections
commodity_raw = st.sidebar.selectbox("Select Commodity", all_comms)
market_options = ["All Markets"] + all_mkts
market = st.sidebar.selectbox("Select Market", market_options)
month = st.sidebar.selectbox("Select Month", ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
price_choice = st.sidebar.radio("Display Price By:", ["Price per Kg", "Price per Bag"])

# 3. Key variables
display_name = format_commodity_name(commodity_raw)
target_col = "price_per_kg" if price_choice == "Price per Kg" else "price_per_bag"

# --- 5. MAIN CONTENT ---
st.title("Commodity Pricing Intelligence Dashboard")
st.subheader(f"Kasuwa Internal Price Trend: {display_name} in {month}")

try:
    # Adding timestamp 'v' to bypass the Render API cache
    timestamp = int(time.time())
    response = requests.get(
        f"{BASE_URL}/analysis", 
        params={"commodity": commodity_raw, "month": month, "market": market, "v": timestamp}, 
        headers=HEADERS
    )
    
    if response.status_code == 200:
        data = response.json()
        chart_data = data.get("chart_data")

        if chart_data:
            df = pd.DataFrame(chart_data)
            df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
            
            # Outlier protection (Hidden local filter for UI)
            df_filtered = df[df[target_col] > 150].copy() if "Soya" in commodity_raw else df.copy()
            
            df['start_time'] = pd.to_datetime(df['start_time'])
            df['day'] = df['start_time'].dt.day
            df['year'] = df['start_time'].dt.year.astype(str)
            dfc_grouped = df.groupby(['day', 'year'])[target_col].mean().reset_index()

            # --- CHART ---
            fig = px.line(dfc_grouped, x="day", y=target_col, color="year", markers=True,
                          text=dfc_grouped[target_col].apply(lambda x: f"<b>{x:,.0f}</b>"),
                          color_discrete_map={"2024": PRIMARY_COLOR, "2025": ACCENT_COLOR, "2026": "#E67E22"},
                          labels={"day": "Day of Month", target_col: f"{price_choice} (₦)"})
            
            fig.update_traces(textposition="top center")
            fig.update_layout(
                plot_bgcolor="white", paper_bgcolor="white", 
                font=dict(color="black", family="Arial Black"),
                xaxis=dict(title=dict(text="<b>Day of Month</b>", font=dict(size=16, color="black")),
                    tickfont=dict(size=14, color="black", family="Arial Black"), 
                    showline=True, linecolor="black", linewidth=3, gridcolor="#eeeeee"),
                yaxis=dict(title=dict(text=f"<b>{price_choice} (₦)</b>", font=dict(size=16, color="black")),
                    tickfont=dict(size=14, color="black", family="Arial Black"), 
                    showline=True, linecolor="black", linewidth=3, gridcolor="#eeeeee")
            )
            st.plotly_chart(fig, use_container_width=True)

            # --- KPIs ---
            avg_val = df_filtered[target_col].mean()
            max_val = df_filtered[target_col].max()
            min_val = df_filtered[target_col].min()

            st.markdown(f"""
                <div class="metric-container">
                    <div class="metric-card">
                        <div class="metric-label">Avg Kasuwa internal price ({price_choice})</div>
                        <div class="metric-value">₦{avg_val:,.0f}</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Highest Kasuwa internal price ({price_choice})</div>
                        <div class="metric-value">₦{max_val:,.0f}</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Lowest Kasuwa internal price ({price_choice})</div>
                        <div class="metric-value">₦{min_val:,.0f}</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)

            # --- ADVISORY ---
            info = COMMODITY_INFO.get(commodity_raw, {"desc": "Detailed market data arriving soon.", "markets": "Regional Hubs", "abundance": "Seasonal", "note": "Monitor daily for updates."})
            st.markdown(f"""
                <div class="advisor-container">
                    <p style="font-size: 18px; color: {PRIMARY_COLOR}; margin-bottom: 5px;">
                        <b>💡 {display_name} Intelligence:</b>
                    </p>
                    <p style="margin: 0; color: #333;">
                        {info['desc']} Primary sourcing markets include <b>{info['markets']}</b>. 
                        Periods of high abundance: <b>{info['abundance']}</b>.
                    </p>
                    <p style="margin-top: 10px; color: #555; font-style: italic;">
                        <b>Market Note:</b> {info['note']}
                    </p>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.warning(f"No data found for {display_name} in {month}.")

except Exception as e:
    st.error(f"UI Error: {e}")