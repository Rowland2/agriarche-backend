import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# --- 1. BRANDING & INTELLIGENCE DATA ---
PRIMARY_COLOR = "#1F7A3F" 
ACCENT_COLOR = "#F4B266"
BG_COLOR = "#F5F7FA"

COMMODITY_INFO = {
    "Soybeans": {"desc": "A raw leguminous crop used for oil and feed.", "markets": "Mubi, Giwa, and Kumo", "abundance": "Nov, Dec, and April", "note": "A key industrial driver for the poultry and vegetable oil sectors."},
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

# Helper to flip names: "Maize White" -> "White Maize"
def format_commodity_name(name):
    parts = name.split()
    colors = ["white", "brown", "red", "yellow", "black"]
    if len(parts) > 1 and parts[-1].lower() in colors:
        return f"{parts[-1].capitalize()} {' '.join(parts[:-1])}"
    return name.capitalize()

st.set_page_config(page_title="Agriarche Intelligence Hub", layout="wide")

# --- 2. CSS ---
st.markdown(f"""
    <style>
        header {{ visibility: hidden; }}
        .stApp {{ background-color: {BG_COLOR}; }}
        section[data-testid="stSidebar"] {{ background-color: {ACCENT_COLOR} !important; }}
        h1, h2, h3 {{ color: {PRIMARY_COLOR} !important; font-family: 'Arial Black'; }}
        .advisor-container {{
            background-color: #FFFFFF; padding: 20px; border-radius: 15px;
            border-left: 8px solid {ACCENT_COLOR}; margin-bottom: 25px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        }}
        .metric-container {{ display: flex; justify-content: space-between; gap: 15px; }}
        .metric-card {{
            background-color: white; padding: 20px; border-radius: 15px;
            border-left: 8px solid {PRIMARY_COLOR}; box-shadow: 2px 4px 10px rgba(0,0,0,0.05); width: 100%;
        }}
        .metric-label {{ font-size: 14px; color: #555; font-weight: bold; }}
        .metric-value {{ font-size: 28px; color: {PRIMARY_COLOR}; font-weight: 800; }}
    </style>
""", unsafe_allow_html=True)

# --- 3. DYNAMIC SIDEBAR FILTERS ---
st.sidebar.title("Market Filters")

# Fetching the full list of commodities and markets from the API
try:
    init_res = requests.get("https://agriarche-backend.onrender.com/analysis", headers={"access_token": "Agriarche_Internal_Key_2026"})
    if init_res.status_code == 200:
        db_data = pd.DataFrame(init_res.json().get("chart_data", []))
        all_commodities = sorted(db_data['commodity'].unique().tolist())
        all_markets = ["All Markets"] + sorted(db_data['market'].unique().tolist())
    else:
        all_commodities = list(COMMODITY_INFO.keys())
        all_markets = ["All Markets", "Biliri", "Dawanau", "Potiskum", "Giwa"]
except:
    all_commodities = list(COMMODITY_INFO.keys())
    all_markets = ["All Markets"]

commodity_raw = st.sidebar.selectbox("Select Commodity", all_commodities)
market = st.sidebar.selectbox("Select Market", all_markets)
month = st.sidebar.selectbox("Select Month", ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
price_choice = st.sidebar.radio("Display Price By:", ["Price per Kg", "Price per Bag"])

target_col = "price_per_kg" if price_choice == "Price per Kg" else "price_per_bag"
display_name = format_commodity_name(commodity_raw)

# --- 4. MAIN CONTENT ---
st.title("Commodity Pricing Intelligence Dashboard")
st.subheader(f"{display_name} Pricing Trend in {month}")

try:
    response = requests.get(f"https://agriarche-backend.onrender.com/analysis", 
                            params={"commodity": commodity_raw, "month": month, "market": market}, 
                            headers={"access_token": "Agriarche_Internal_Key_2026"})
    
    if response.status_code == 200:
        data = response.json()
        metrics = data.get("metrics")
        chart_data = data.get("chart_data")

        if chart_data:
            df = pd.DataFrame(chart_data)
            df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
            df['day'] = pd.to_datetime(df['start_time']).dt.day
            dfc_grouped = df.groupby('day')[target_col].mean().reset_index()

            # Chart Logic
            fig = px.line(dfc_grouped, x="day", y=target_col, markers=True,
                          text=dfc_grouped[target_col].apply(lambda x: f"<b>{x:,.0f}</b>"))
            fig.update_traces(line_color=ACCENT_COLOR, line_width=4, textposition="top center")
            fig.update_layout(plot_bgcolor="white", paper_bgcolor="white", font=dict(color="black", family="Arial Black"))
            st.plotly_chart(fig, use_container_width=True)

            # --- INSIGHTS ---
            info = COMMODITY_INFO.get(commodity_raw, {"desc": "Data available in market reports.", "markets": "Local Hubs", "abundance": "Seasonal", "note": "Check back for updates."})
            st.markdown(f"""
                <div class="advisor-container">
                    <h4 style="margin-top:0; color:{PRIMARY_COLOR};">Market Insights: {display_name}</h4>
                    <p><b>Description:</b> {info['desc']}</p>
                    <p><b>Primary Markets:</b> {info['markets']}</p>
                    <p><b>Peak Abundance:</b> {info['abundance']}</p>
                    <p style="color: #666; font-style: italic; border-top: 1px solid #eee; padding-top:10px;"><b>Note:</b> {info['note']}</p>
                </div>
            """, unsafe_allow_html=True)

            # --- KPI CARDS ---
            st.markdown(f"""
                <div class="metric-container">
                    <div class="metric-card"><div class="metric-label">Avg Price</div><div class="metric-value">₦{metrics['avg']:,.0f}</div></div>
                    <div class="metric-card"><div class="metric-label">Max Price</div><div class="metric-value">₦{metrics['max']:,.0f}</div></div>
                    <div class="metric-card"><div class="metric-label">Min Price</div><div class="metric-value">₦{metrics['min']:,.0f}</div></div>
                </div>
            """, unsafe_allow_html=True)
except Exception as e:
    st.error(f"UI Error: {e}")