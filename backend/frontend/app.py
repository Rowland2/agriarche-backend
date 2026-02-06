import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import time

# --- 1. BRANDING & INFO ---
PRIMARY_COLOR = "#1F7A3F" 
ACCENT_COLOR = "#F4B266"  
BG_COLOR = "#F5F7FA"      

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

HARDCODED_COMMODITIES = sorted(list(COMMODITY_INFO.keys()))
HARDCODED_MARKETS = ["Biliri", "Dawanau", "Giwa", "Kumo", "Lashe Money", "Pambegua", "Potiskum", "Sabo Kasuwa Mubi"]

def format_commodity_name(name):
    parts = name.split()
    colors = ["white", "brown", "red", "yellow", "black"]
    if len(parts) > 1 and parts[-1].lower() in colors:
        return f"{parts[-1].capitalize()} {' '.join(parts[:-1])}"
    return name.capitalize()

st.set_page_config(page_title="Agriarche Intelligence Hub", layout="wide")

st.markdown(f"""
    <style>
        header {{ visibility: hidden; }}
        .stApp {{ background-color: {BG_COLOR}; }}
        section[data-testid="stSidebar"] {{ background-color: {ACCENT_COLOR} !important; }}
        section[data-testid="stSidebar"] div[data-baseweb="select"] > div {{ background-color: #FFFFFF !important; color: #000000 !important; }}
        h1, h2, h3 {{ color: {PRIMARY_COLOR} !important; }}
        .advisor-container {{ background-color: #FFFFFF; padding: 20px; border-radius: 10px; border-left: 5px solid {ACCENT_COLOR}; margin-top: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
        .metric-container {{ display: flex; justify-content: space-between; gap: 15px; margin-top: 30px; }}
        .metric-card {{ background-color: white; padding: 20px; border-radius: 15px; border-left: 8px solid {PRIMARY_COLOR}; box-shadow: 2px 4px 10px rgba(0,0,0,0.05); width: 100%; }}
        .metric-label {{ font-size: 14px; color: #555; font-weight: bold; }}
        .metric-value {{ font-size: 28px; color: {PRIMARY_COLOR}; font-weight: 800; }}
    </style>
""", unsafe_allow_html=True)

# --- API CONFIG ---
BASE_URL = "https://agriarche-backend.onrender.com"
HEADERS = {"access_token": "Agriarche_Internal_Key_2026"}

# --- SIDEBAR ---
st.sidebar.title("Market Filters")
commodity_raw = st.sidebar.selectbox("Select Commodity", HARDCODED_COMMODITIES)
market = st.sidebar.selectbox("Select Market", ["All Markets"] + HARDCODED_MARKETS)
month = st.sidebar.selectbox("Select Month", ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
price_choice = st.sidebar.radio("Display Price By:", ["Price per Kg", "Price per Bag"])

display_name = format_commodity_name(commodity_raw)
target_col = "price_per_kg" if price_choice == "Price per Kg" else "price_per_bag"

# --- MAIN DASHBOARD ---
st.title("Commodity Pricing Intelligence Dashboard")
st.subheader(f"Kasuwa Internal Price Trend: {display_name} in {month}")

try:
    timestamp = int(time.time())
    response = requests.get(f"{BASE_URL}/analysis", 
                            params={"commodity": commodity_raw, "month": month, "market": market, "v": timestamp}, 
                            headers=HEADERS)
    
    if response.status_code == 200:
        data = response.json()
        chart_data = data.get("chart_data")

        if chart_data:
            df = pd.DataFrame(chart_data)
            df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
            
            # Formatting for KPIs and Charts
            df['start_time'] = pd.to_datetime(df['start_time'])
            df['day'] = df['start_time'].dt.day
            df['year'] = df['start_time'].dt.year.astype(str)
            dfc_grouped = df.groupby(['day', 'year'])[target_col].mean().reset_index()

            # Chart Logic
            fig = px.line(dfc_grouped, x="day", y=target_col, color="year", markers=True,
                          text=dfc_grouped[target_col].apply(lambda x: f"<b>{x:,.0f}</b>"),
                          color_discrete_map={"2024": PRIMARY_COLOR, "2025": ACCENT_COLOR, "2026": "#E67E22"})
            fig.update_layout(plot_bgcolor="white", paper_bgcolor="white", font=dict(family="Arial Black"))
            st.plotly_chart(fig, use_container_width=True)

            # Metric Cards
            avg_val, max_val, min_val = df[target_col].mean(), df[target_col].max(), df[target_col].min()
            st.markdown(f"""
                <div class="metric-container">
                    <div class="metric-card"><div class="metric-label">Avg price</div><div class="metric-value">₦{avg_val:,.0f}</div></div>
                    <div class="metric-card"><div class="metric-label">High price</div><div class="metric-value">₦{max_val:,.0f}</div></div>
                    <div class="metric-card"><div class="metric-label">Low price</div><div class="metric-value">₦{min_val:,.0f}</div></div>
                </div>
            """, unsafe_allow_html=True)

            # Advisory Container
            info = COMMODITY_INFO.get(commodity_raw, {"desc": "Data coming soon.", "markets": "Hubs", "abundance": "Seasonal", "note": "Monitor daily."})
            st.markdown(f"""<div class="advisor-container"><b>💡 {display_name} Intelligence:</b><br>{info['desc']} Markets: {info['markets']}.<br><i>Note: {info['note']}</i></div>""", unsafe_allow_html=True)

            # --- 📚 NEW: DATA ARCHIVE SECTION (As per Screenshot 1153) ---
            st.markdown("---")
            st.subheader("📚 Kasuwa internal price Data Archive")
            st.write("Search through all Kasuwa internal price records regardless of sidebar filters.")
            
            hist_search = st.text_input("🔍 Search Kasuwa internal price Records", placeholder="Search by market, year, or commodity...", key="hist_search_bar")
            
            # Using the full dataframe for the archive
            hist_display = df.copy()
            
            # Standardizing Column Names to match your Screenshot requirements
            if "start_time" in hist_display.columns:
                hist_display["Date"] = pd.to_datetime(hist_display["start_time"]).dt.strftime('%Y-%m-%d')
            
            hist_display["Price/KG (₦)"] = hist_display["price_per_kg"] if "price_per_kg" in hist_display.columns else 0
            hist_display["Total Price (₦)"] = hist_display["price"] if "price" in hist_display.columns else 0
            
            # Selecting and Renaming based on Screenshot (1153)
            display_cols = ["Date", "commodity", "market", "Price/KG (₦)", "Total Price (₦)", "year", "month_name"]
            hist_display = hist_display[[c for c in display_cols if c in hist_display.columns]]
            hist_display = hist_display.rename(columns={"commodity": "Commodity", "market": "Market"})

            if hist_search:
                mask = hist_display.apply(lambda row: row.astype(str).str.contains(hist_search, case=False).any(), axis=1)
                hist_display = hist_display[mask]
            
            st.dataframe(
                hist_display.sort_values(by="Date", ascending=False).style.format({
                    "Price/KG (₦)": "{:,.2f}",
                    "Total Price (₦)": "{:,.0f}"
                }),
                use_container_width=True,
                hide_index=True
            )

        else:
            st.warning(f"No data found for {display_name} in {month}.")
except Exception as e:
    st.error(f"UI Error: {e}")