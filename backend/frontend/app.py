import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import time
from datetime import datetime
from io import BytesIO

# Import for PDF Generation
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import Flowable

# =====================================================
# 1. BRANDING & DATA
# =====================================================
PRIMARY_COLOR = "#1F7A3F" 
ACCENT_COLOR = "#F4B266"  
BG_COLOR = "#F5F7FA"
LOGO_PATH = "assets/logo.png"  # Optional: add your logo

COMMODITY_INFO = {
    "Soybeans": {"desc": "A raw leguminous crop used for oil and feed.", "markets": "Mubi, Giwa, and Kumo", "abundance": "Nov, Dec, and April", "note": "A key industrial driver for the poultry and vegetable oil sectors."},
    "Brown Cowpea": {"desc": "Protein-rich legume popular in local diets.", "markets": "Dawanau and Potiskum", "abundance": "Oct through Jan", "note": "Supply depends on Northern storage."},
    "White Cowpea": {"desc": "Staple bean variety used for commercial flour.", "markets": "Dawanau and Bodija", "abundance": "Oct and Nov", "note": "High demand in South drives prices."},
    "Honey beans": {"desc": "Premium sweet brown beans (Oloyin).", "markets": "Oyingbo and Dawanau", "abundance": "Oct to Dec", "note": "Often carries a price premium."},
    "White Maize": {"desc": "Primary cereal crop for food and industry.", "markets": "Giwa, Makarfi, and Funtua", "abundance": "Sept to Nov", "note": "Correlates strongly with Sorghum trends."},
    "Rice Paddy": {"desc": "Raw rice before milling/processing.", "markets": "Argungu and Kano", "abundance": "Nov and Dec", "note": "Foundations for processed rice pricing."},
    "Processed Rice": {"desc": "Milled and polished local rice.", "markets": "Kano, Lagos, and Onitsha", "abundance": "Year-round", "note": "Price fluctuates with fuel/milling costs."},
    "Red Sorghum": {"desc": "Drought-resistant grain staple.", "markets": "Dawanau and Gombe", "abundance": "Dec and Jan", "note": "Market substitute for Maize."},
    "White Sorghum": {"desc": "Drought-resistant white grain variety.", "markets": "Dawanau and Gombe", "abundance": "Dec and Jan", "note": "Premium variety for food processing."},
    "Yellow Sorghum": {"desc": "Yellow grain sorghum variety.", "markets": "Dawanau and Gombe", "abundance": "Dec and Jan", "note": "Used for brewing and animal feed."},
    "Sorghum": {"desc": "General sorghum variety.", "markets": "Dawanau and Gombe", "abundance": "Dec and Jan", "note": "Versatile grain for food and industry."},
    "Millet": {"desc": "Fast-growing cereal for the lean season.", "markets": "Dawanau and Potiskum", "abundance": "Sept and Oct", "note": "First harvest after rainy season."},
    "Groundnut gargaja": {"desc": "Local peanut variety for oil extraction.", "markets": "Dawanau and Gombe", "abundance": "Oct and Nov", "note": "Sahel region specialty."},
    "Groundnut kampala": {"desc": "Large, premium roasting groundnuts.", "markets": "Kano and Dawanau", "abundance": "Oct and Nov", "note": "Higher oil content than Gargaja."}
}

# Function to normalize commodity names (color first)
def normalize_commodity_for_display(name):
    """Normalize to match COMMODITY_INFO keys with color first"""
    name_lower = name.lower().strip()
    
    # Map common variations to standardized names
    if "cowpea" in name_lower and "brown" in name_lower:
        return "Brown Cowpea"
    elif "cowpea" in name_lower and "white" in name_lower:
        return "White Cowpea"
    elif "maize" in name_lower and "white" in name_lower:
        return "White Maize"
    elif "sorghum" in name_lower and "red" in name_lower:
        return "Red Sorghum"
    elif "sorghum" in name_lower and "white" in name_lower:
        return "White Sorghum"
    elif "sorghum" in name_lower and "yellow" in name_lower:
        return "Yellow Sorghum"
    elif name_lower == "sorghum":
        return "Sorghum"
    elif "soya" in name_lower or "soy" in name_lower:
        return "Soybeans"
    elif "honey" in name_lower:
        return "Honey Beans"
    elif "rice" in name_lower and "paddy" in name_lower:
        return "Rice Paddy"
    elif "rice" in name_lower and "process" in name_lower:
        return "Processed Rice"
    elif "millet" in name_lower:
        return "Millet"
    elif "groundnut" in name_lower and "gargaja" in name_lower:
        return "Groundnut Gargaja"
    elif "groundnut" in name_lower and "kampala" in name_lower:
        return "Groundnut kampala"
    
    return name

def convert_display_to_api_format(display_name):
    """Convert display name (Color First) back to API format (Color Last) for querying"""
    name = display_name.strip()
    
    # Map display names to API format
    mappings = {
        "Brown Cowpea": "Cowpea Brown",
        "White Cowpea": "Cowpea White",
        "White Maize": "Maize White",
        "Red Sorghum": "Sorghum Red",
        "White Sorghum": "Sorghum White",
        "Yellow Sorghum": "Sorghum Yellow",
        "Sorghum": "Sorghum",
       "Soybeans": "Soybeans",
        "Honey beans": "Honey Beans",
        "Rice Paddy": "Rice Paddy",
        "Processed Rice": "Rice processed",
        "Millet": "Millet",
        "Groundnut gargaja": "Groundnut Gargaja",
        "Groundnut kampala": "Groundnut kampala"
    }
    
    return mappings.get(name, name)

HARDCODED_COMMODITIES = sorted(list(COMMODITY_INFO.keys()))
HARDCODED_MARKETS = ["Biliri", "Dawanau", "Giwa", "Kumo", "Lashe Money", "Pambegua", "Potiskum", "Sabo Kasuwa Mubi"]

def format_commodity_name(name):
    """Format commodity names to put color adjectives FIRST"""
    parts = name.split()
    colors_list = ["white", "brown", "red", "yellow", "black"]
    
    # Check if last word is a color
    if len(parts) > 1 and parts[-1].lower() in colors_list:
        # Move color to the front: "Cowpea White" -> "White Cowpea"
        color = parts[-1].capitalize()
        commodity = ' '.join(parts[:-1])
        return f"{color} {commodity}"
    
    return name.capitalize()

st.set_page_config(page_title="Agriarche Intelligence Hub", layout="wide")

# =====================================================
# 2. CSS STYLING
# =====================================================
st.markdown(f"""
    <style>
        header {{ visibility: hidden; }}
        .stApp {{ background-color: {BG_COLOR}; }}
        section[data-testid="stSidebar"] {{ background-color: {ACCENT_COLOR} !important; }}
        section[data-testid="stSidebar"] div[data-baseweb="select"] > div {{ background-color: #FFFFFF !important; color: #000000 !important; }}
        h1, h2, h3 {{ color: {PRIMARY_COLOR} !important; }}
        
        /* KPI Card Styling */
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
        
        /* Advisor Container */
        .advisor-container {{
            background-color: #FFFFFF;
            padding: 20px;
            border-radius: 10px;
            border-left: 5px solid {ACCENT_COLOR};
            margin-top: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }}
    </style>
""", unsafe_allow_html=True)

# =====================================================
# 3. PDF GENERATOR FUNCTION
# =====================================================
class HorizontalLine(Flowable):
    def __init__(self, width, height, color):
        Flowable.__init__(self)
        self.width = width
        self.height = height
        self.color = color
    def draw(self):
        self.canv.setStrokeColor(self.color)
        self.canv.line(0, 0, self.width, 0)

def generate_pdf_report(month_name, report_df):
    """Generate a comprehensive PDF report for a given month"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    
    # Custom Styles
    title_style = ParagraphStyle('TitleStyle', parent=styles['Heading1'], 
                                 textColor=colors.HexColor(PRIMARY_COLOR), spaceAfter=12)
    market_header_style = ParagraphStyle('MarketHeader', parent=styles['Heading2'], 
                                        textColor=colors.HexColor(PRIMARY_COLOR), 
                                        spaceBefore=15, spaceAfter=5)
    sub_style = ParagraphStyle('SubStyle', parent=styles['Heading3'], 
                               textColor=colors.black, spaceBefore=8)
    body_style = styles['Normal']
    
    elements = []
    
    # --- REPORT HEADER ---
    elements.append(Paragraph(f"Agriarche Market Intelligence Report", title_style))
    elements.append(Paragraph(f"Analysis Month: {month_name}", styles['Heading3']))
    elements.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M')}", body_style))
    elements.append(Spacer(1, 20))

    # --- WATERMARK LOGIC WITH LOGO ---
    def add_watermark(canvas, doc):
        canvas.saveState()
        # Add logo watermark if exists
        if os.path.exists(LOGO_PATH):
            canvas.setFillAlpha(0.1)
            canvas.drawImage(LOGO_PATH, letter[0]/2 - 1.5*inch, letter[1]/2 - 1.5*inch, 
                           width=3*inch, preserveAspectRatio=True, mask='auto')
        canvas.restoreState()

    # --- CONTENT GENERATION (GROUPED BY MARKET) ---
    # Use "N" instead of ₦ symbol for PDF compatibility
    summary_table_data = [["Market", "Commodity", "Avg Price/Kg (N)", "High/Kg (N)", "Low/Kg (N)"]]
    unique_markets = sorted(report_df["market"].unique())

    for market in unique_markets:
        market_df = report_df[report_df["market"] == market]
        elements.append(Paragraph(f"Location: {market}", market_header_style))
        elements.append(HorizontalLine(6.5*inch, 1, colors.grey))
        
        for comm in sorted(market_df["commodity"].unique()):
            comm_df = market_df[market_df["commodity"] == comm]
            
            avg_p = comm_df["price_per_kg"].mean()
            high_p = comm_df["price_per_kg"].max()
            low_p = comm_df["price_per_kg"].min()
            
            elements.append(Paragraph(f"<b>{comm}</b>", sub_style))
            # Use "N" instead of ₦ for PDF compatibility
            text = (f"In {market}, the average price for {comm} was <b>N{avg_p:,.2f}/Kg</b>. "
                    f"Prices peaked at N{high_p:,.2f}/Kg with a floor of N{low_p:,.2f}/Kg.")
            elements.append(Paragraph(text, body_style))
            
            summary_table_data.append([market, comm, f"{avg_p:,.2f}", f"{high_p:,.2f}", f"{low_p:,.2f}"])
        
        elements.append(Spacer(1, 10))

    # --- PRICE SUMMARY TABLE ---
    elements.append(Spacer(1, 15))
    elements.append(Paragraph("Comprehensive Market Summary Table", market_header_style))
    elements.append(Spacer(1, 10))

    t = Table(summary_table_data, colWidths=[110, 140, 90, 80, 80], repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(ACCENT_COLOR)),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey])
    ]))
    elements.append(t)

    doc.build(elements, onFirstPage=add_watermark, onLaterPages=add_watermark)
    buffer.seek(0)
    return buffer

# =====================================================
# 4. API CONFIG
# =====================================================
BASE_URL = "https://agriarche-backend.onrender.com"
HEADERS = {"access_token": "Agriarche_Internal_Key_2026"}

# =====================================================
# 5. SIDEBAR (WITH DYNAMIC FILTERS FROM BACKEND)
# =====================================================

st.sidebar.title("Internal Market Price Filters")

# Fetch filter options from backend (cached for performance)
@st.cache_data(ttl=3600)  # Cache for 1 hour
def fetch_filter_options():
    """Fetch all filter options from backend"""
    try:
        response = requests.get(f"{BASE_URL}/filters/all", headers=HEADERS, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            return {
                "commodities": HARDCODED_COMMODITIES,
                "markets": HARDCODED_MARKETS,
                "states": ["Kaduna", "Kano", "Lagos"],
                "years": ["2024", "2025", "2026"],
                "months": ["January", "February", "March", "April", "May", "June", 
                          "July", "August", "September", "October", "November", "December"]
            }
    except Exception as e:
        print(f"Failed to fetch filters from backend: {e}")
        return {
            "commodities": HARDCODED_COMMODITIES,
            "markets": HARDCODED_MARKETS,
            "states": ["Kaduna", "Kano", "Lagos"],
            "years": ["2024", "2025", "2026"],
            "months": ["January", "February", "March", "April", "May", "June", 
                      "July", "August", "September", "October", "November", "December"]
        }

# Get filter options from backend
filter_options = fetch_filter_options()

# Normalize commodity names for display (Color First)
commodities_raw = filter_options['commodities']

# Clean and deduplicate commodities (case-insensitive)
commodities_cleaned = {}
for c in commodities_raw:
    normalized = normalize_commodity_for_display(c)
    key = normalized.lower()
    if key not in commodities_cleaned:
        commodities_cleaned[key] = normalized

commodities_display = sorted(list(commodities_cleaned.values()))

# ✅ ADD "All Commodities" as the first option
commodities_display = ["All Commodities"] + commodities_display

# Sidebar dropdowns
commodity_raw = st.sidebar.selectbox("Select Commodity", commodities_display)
market_sel = st.sidebar.selectbox("Select Market", ["All Markets"] + filter_options['markets'])

current_month = datetime.now().strftime("%B")
months_list = filter_options['months']
default_month_index = months_list.index(current_month) if current_month in months_list else 0
month_sel = st.sidebar.selectbox("Select Month", months_list, index=default_month_index)

# Year filter
years_list = filter_options.get('years', [ "2025", "2026"])
selected_years = st.sidebar.multiselect("Year", years_list, default=["2026"], key="main_years")
price_choice = st.sidebar.radio("Display Price By:", ["Price per Kg", "Price per Bag"])

display_name = format_commodity_name(commodity_raw) if commodity_raw != "All Commodities" else "All Commodities"
target_col = "price_per_kg" if price_choice == "Price per Kg" else "price_per_bag"

# ✅ If "All Commodities" selected, send None to API (returns everything)
api_commodity_name = None if commodity_raw == "All Commodities" else convert_display_to_api_format(commodity_raw)

# =====================================================
# 6. MAIN CONTENT (CHART & KPIs)
# =====================================================
# Display logo at top
try:
    import os
    if os.path.exists(LOGO_PATH):
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.image(LOGO_PATH, use_container_width=True)
    else:
        st.markdown("""
            <div style='text-align: center; padding: 20px;'>
                <h1 style='color: #1F7A3F; font-size: 48px; font-weight: bold; margin: 0;'>
                    🌾 AGRIARCHE
                </h1>
            </div>
        """, unsafe_allow_html=True)
except Exception as e:
    st.markdown("""
        <div style='text-align: center; padding: 20px;'>
            <h1 style='color: #1F7A3F; font-size: 48px; font-weight: bold; margin: 0;'>
                🌾 AGRIARCHE
            </h1>
        </div>
    """, unsafe_allow_html=True)

st.title("Commodity Pricing Intelligence Dashboard")
st.subheader(f"Market Price Trend: {display_name} in {month_sel}")

# ✅ Skip chart if "All Commodities" is selected
if api_commodity_name is None:
    st.info("📊 Select a specific commodity from the sidebar to view the price trend chart and market intelligence.")
else:
    try:
        timestamp = int(time.time())
        response = requests.get(f"{BASE_URL}/analysis",
                                params={"commodity": api_commodity_name, "month": month_sel, "market": market_sel, "v": timestamp},
                                headers=HEADERS)

        if response.status_code == 200:
            data = response.json()
            chart_data = data.get("chart_data", [])

            if chart_data:
                df = pd.DataFrame(chart_data)
                df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
                df['start_time'] = pd.to_datetime(df['start_time'])
                df['day'] = df['start_time'].dt.day
                df['year'] = df['start_time'].dt.year.astype(str)
                dfc_grouped = df.groupby(['day', 'year'])[target_col].mean().reset_index()

                fig = px.line(dfc_grouped, x="day", y=target_col, color="year", markers=True,
                              text=dfc_grouped[target_col].apply(lambda x: f"<b>{x:,.0f}</b>"),
                              color_discrete_map={"2024": PRIMARY_COLOR, "2025": ACCENT_COLOR, "2026": "#E67E22"},
                              labels={"day": "Day of Month", target_col: "Price (₦)"})

                fig.update_traces(textposition="top center")
                fig.update_layout(
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(color="black", family="Arial Black"),
                    xaxis=dict(
                        title=dict(text="<b>Day of Month</b>", font=dict(size=16, color="black")),
                        tickfont=dict(size=14, color="black", family="Arial Black"),
                        showline=True, linecolor="black", linewidth=3, gridcolor="#eeeeee",
                        dtick=1
                    ),
                    yaxis=dict(
                        title=dict(text=f"<b>Price (₦)</b>", font=dict(size=16, color="black")),
                        tickfont=dict(size=14, color="black", family="Arial Black"),
                        showline=True, linecolor="black", linewidth=3, gridcolor="#eeeeee"
                    )
                )
                st.plotly_chart(fig, use_container_width=True)

                avg_val, max_val, min_val = df[target_col].mean(), df[target_col].max(), df[target_col].min()
                st.markdown(f"""
                    <div class="metric-container">
                        <div class="metric-card"><div class="metric-label">Avg price</div><div class="metric-value">₦{avg_val:,.0f}</div></div>
                        <div class="metric-card"><div class="metric-label">High price</div><div class="metric-value">₦{max_val:,.0f}</div></div>
                        <div class="metric-card"><div class="metric-label">Low price</div><div class="metric-value">₦{min_val:,.0f}</div></div>
                    </div>
                """, unsafe_allow_html=True)

                info = COMMODITY_INFO.get(commodity_raw, {"desc": "", "markets": "", "abundance": "", "note": ""})
                if not info.get("desc"):
                    normalized = normalize_commodity_for_display(commodity_raw)
                    info = COMMODITY_INFO.get(normalized, {"desc": "Market data profiling in progress...", "markets": "Northern Hubs", "abundance": "Seasonal", "note": "Monitoring price shifts."})

                st.markdown(f"""
                    <div class="advisor-container" style="border-left: 5px solid {ACCENT_COLOR};">
                        <p style="color: #1F2937; font-size: 17px; margin: 0; line-height: 1.8;">
                            <b style="font-size: 18px;">🌾 {display_name} Intelligence:</b><br><br>
                            {info['desc']}<br><br>
                            <b>Primary Markets:</b> {info['markets']}<br>
                            <b>Peak Abundance:</b> {info['abundance']}<br><br>
                            <i style="color: #666;">💡 Market Note: {info['note']}</i>
                        </p>
                    </div>
                """, unsafe_allow_html=True)

            else:
                st.warning(f"No chart data found for {display_name} in {month_sel}.")

    except Exception as e:
        st.error(f"Chart error: {str(e)}")

# =====================================================
# STRATEGIC SOURCING - MOVED HERE (after Commodity Intelligence)
# =====================================================
try:
    all_response = requests.get(f"{BASE_URL}/prices", params={"page": 1, "page_size": 10000}, headers=HEADERS, timeout=20)
    if all_response.status_code == 200:
        response_data = all_response.json()

        if isinstance(response_data, dict) and 'data' in response_data:
            all_data = pd.DataFrame(response_data['data'])
        else:
            all_data = pd.DataFrame(response_data)

        all_data['start_time'] = pd.to_datetime(all_data['start_time'])
        all_data['month_name'] = all_data['start_time'].dt.strftime('%B')
        all_data['price_per_kg'] = pd.to_numeric(all_data['price_per_kg'], errors='coerce')
        all_data['price_per_bag'] = pd.to_numeric(all_data['price_per_bag'], errors='coerce')

        report_data = all_data[all_data['month_name'] == month_sel].copy()

        if api_commodity_name is not None:
            strategy_df = report_data[report_data["commodity"].str.lower() == api_commodity_name.lower()].copy()

            if not strategy_df.empty and len(strategy_df['market'].unique()) > 1:
                strategy_df[target_col] = pd.to_numeric(strategy_df[target_col], errors='coerce')
                m_ranks = strategy_df.groupby("market")[target_col].mean().sort_values()
                best_m = m_ranks.index[0]
                best_p = m_ranks.iloc[0]
                worst_m = m_ranks.index[-1]
                worst_p = m_ranks.iloc[-1]

                unit_label = "Avg/Kg" if price_choice == "Price per Kg" else "Avg/Bag"

                st.markdown("<br>", unsafe_allow_html=True)
                st.subheader(f"🎯 Strategic Sourcing: {display_name}")
                scol1, scol2 = st.columns(2)
                with scol1:
                    st.markdown(f"""
                        <div class="strategy-card best-buy">
                            <div style="font-size: 14px; opacity: 0.9;">CHEAPEST MARKET (BEST TO BUY)</div>
                            <div style="font-size: 24px; font-weight: bold; margin: 5px 0;">{best_m}</div>
                            <div style="font-size: 20px;">₦{best_p:,.2f} <small>({unit_label})</small></div>
                        </div>
                    """, unsafe_allow_html=True)
                with scol2:
                    st.markdown(f"""
                        <div class="strategy-card worst-buy">
                            <div style="font-size: 14px; opacity: 0.9;">HIGHEST PRICE MARKET (AVOID)</div>
                            <div style="font-size: 24px; font-weight: bold; margin: 5px 0;">{worst_m}</div>
                            <div style="font-size: 20px;">₦{worst_p:,.2f} <small>({unit_label})</small></div>
                        </div>
                    """, unsafe_allow_html=True)

except Exception as e:
    pass  # Strategic sourcing not critical, continue if fails

# =====================================================
# 7. STANDALONE DATA ARCHIVE TABLE (WITH PROPER PAGINATION)
# =====================================================
st.markdown("---")
st.subheader("📚 Internal Market Price Data Archive")

# Initialize page state
if "archive_page" not in st.session_state:
    st.session_state.archive_page = 1

try:
    archive_col1, archive_col2, archive_col3 = st.columns([2,2,3])
    
    with archive_col1:
        archive_page_size = st.selectbox(
            "Records per page",
            [50,100,200,500],
            index=1,
            key="archive_page_size"
        )
    
    with archive_col2:
        st.write(f"Current Page: {st.session_state.archive_page}")
    
    with archive_col3:
        hist_search = st.text_input(
            "🔍 Search",
            placeholder="Search by market, year, or commodity..."
        )
    
    # Build params with session state
    params = {
        "page": st.session_state.archive_page,
        "page_size": archive_page_size
    }
    
    full_res = requests.get(f"{BASE_URL}/prices", params=params, headers=HEADERS, timeout=15)
    
    if full_res.status_code == 200:
        result = full_res.json()
        all_raw_data = result.get('data', [])
        pagination = result.get('pagination', {})
        
        if all_raw_data:
            df_hist = pd.DataFrame(all_raw_data)
            
            # Formatting and cleaning columns
            df_hist["Date"] = pd.to_datetime(df_hist["start_time"])
            df_hist["Price per Kg (₦)"] = pd.to_numeric(df_hist["price_per_kg"], errors='coerce')
            df_hist["Price per Bag (₦)"] = pd.to_numeric(df_hist["price_per_bag"], errors='coerce')
            
            # Apply commodity name normalization
            df_hist["commodity"] = df_hist["commodity"].apply(normalize_commodity_for_display)
            
            # Sort by date, commodity, and market
            df_hist = df_hist.sort_values(['commodity', 'market', 'Date'])
            
            # Calculate price change
            df_hist['Previous Price (₦)'] = df_hist.groupby(['commodity', 'market'])['Price per Kg (₦)'].shift(1)
            df_hist['% Change'] = ((df_hist['Price per Kg (₦)'] - df_hist['Previous Price (₦)']) / df_hist['Previous Price (₦)'] * 100).round(2)
            
            # Format date for display
            df_hist["Date_Display"] = df_hist["Date"].dt.strftime('%Y-%m-%d')

            # Select columns to display
            display_cols = ["Date_Display", "commodity", "market", "Price per Kg (₦)", "Price per Bag (₦)", "% Change"]
            hist_display = df_hist[display_cols].copy()
            
            # Rename for display
            hist_display = hist_display.rename(columns={
                "Date_Display": "Date",
                "commodity": "Commodity", 
                "market": "Market"
            })

            # Apply Search Filter
            if hist_search:
                mask = hist_display.apply(lambda row: row.astype(str).str.contains(hist_search, case=False).any(), axis=1)
                hist_display = hist_display[mask]
            
            # Display table
            st.dataframe(
                hist_display.sort_values(by="Date", ascending=False).style.format({
                    "Price per Kg (₦)": "{:,.2f}",
                    "Price per Bag (₦)": "{:,.0f}",
                    "% Change": lambda x: f"{x:+.2f}%" if pd.notna(x) else "—"
                }),
                use_container_width=True,
                hide_index=True,
                height=400
            )
            
            # Pagination info
            st.caption(f"Showing page {pagination.get('page', 1)} of {pagination.get('total_pages', 1)} | Total records: {pagination.get('total_records', 0):,}")
            
            # Navigation buttons
            nav_col1, nav_col2, nav_col3 = st.columns([1,2,1])
            
            with nav_col1:
                if pagination.get('has_previous', False):
                    if st.button("⬅ Previous", key="archive_prev"):
                        st.session_state.archive_page -= 1
                        st.rerun()
            
            with nav_col2:
                st.write(f"Page {pagination.get('page',1)} of {pagination.get('total_pages',1)}")
            
            with nav_col3:
                if pagination.get('has_next', False):
                    if st.button("Next ➡", key="archive_next"):
                        st.session_state.archive_page += 1
                        st.rerun()
        else:
            st.info("No records available in the database archive.")
    else:
        st.error(f"Failed to fetch archive data. Status code: {full_res.status_code}")
        
except Exception as e:
    st.error(f"Archive Table Error: {e}")

# =====================================================
# 8. MONTHLY INTELLIGENCE REPORT & STRATEGIC SOURCING
# =====================================================
st.markdown("---")
st.header(f"📋 Monthly Intelligence Report: {month_sel}")

# Fetch month-specific data for PDF and analysis
try:
    # Get ALL data for monthly report (use large page size)
    month_response = requests.get(f"{BASE_URL}/prices", params={"page": 1, "page_size": 10000}, headers=HEADERS, timeout=20)
    if month_response.status_code == 200:
        response_data = month_response.json()
        
        # Handle paginated response
        if isinstance(response_data, dict) and 'data' in response_data:
            all_data = pd.DataFrame(response_data['data'])
        else:
            all_data = pd.DataFrame(response_data)
        
        all_data['start_time'] = pd.to_datetime(all_data['start_time'])
        all_data['month_name'] = all_data['start_time'].dt.strftime('%B')
        all_data['price_per_kg'] = pd.to_numeric(all_data['price_per_kg'], errors='coerce')
        all_data['price_per_bag'] = pd.to_numeric(all_data['price_per_bag'], errors='coerce')
        
        # Filter for selected month
        report_data = all_data[all_data['month_name'] == month_sel].copy()
        
        if not report_data.empty:
            # PDF Download Button
            pdf_buffer = generate_pdf_report(month_sel, report_data)
            st.download_button(
                label="📥 Download Monthly Intelligence Report (PDF)",
                data=pdf_buffer,
                file_name=f"Agriarche_Market_Report_{month_sel}.pdf",
                mime="application/pdf"
            )

            st.markdown("<br>", unsafe_allow_html=True)

            # STRATEGIC SOURCING CARDS (responds to Price per Kg/Bag toggle)
            strategy_df = report_data[report_data["commodity"].str.lower() == api_commodity_name.lower()].copy()
            
            if not strategy_df.empty and len(strategy_df['market'].unique()) > 1:
                # Use the same target_col as selected in sidebar (price_per_kg or price_per_bag)
                strategy_df[target_col] = pd.to_numeric(strategy_df[target_col], errors='coerce')
                m_ranks = strategy_df.groupby("market")[target_col].mean().sort_values()
                best_m = m_ranks.index[0]
                best_p = m_ranks.iloc[0]
                worst_m = m_ranks.index[-1]
                worst_p = m_ranks.iloc[-1]
                
                # Display unit based on selection
                unit_label = "Avg/Kg" if price_choice == "Price per Kg" else "Avg/Bag"

                # AI MARKET ADVISOR (without repeating Strategic Sourcing cards)
                # AI MARKET ADVISOR - REPLACE THIS ENTIRE SECTION
# ========================================   
                # AI MARKET ADVISOR
                # =====================================================
                st.markdown("<br>", unsafe_allow_html=True)
                st.subheader("🤖 AI Market Advisor")

                try:
                    advisor_response = requests.get(
                        f"{BASE_URL}/ai-market-advisor/{api_commodity_name}",
                        params={"month": month_sel},
                        headers=HEADERS,
                        timeout=10
                    )

                    if advisor_response.status_code == 200:
                        advisor_data = advisor_response.json()

                        advice = advisor_data.get('advice', 'No advice available')
                        confidence = advisor_data.get('confidence', 'low')
                        trend = advisor_data.get('trend', 'stable')

                        if trend == 'rising':
                            bg_adv = "#FFF4E5"
                        elif trend == 'falling':
                            bg_adv = "#E8F5E9"
                        else:
                            bg_adv = "#E3F2FD"

                        st.markdown(f"""
                            <div class="advisor-container" style="background-color: {bg_adv};">
                                <p style="color: #1F2937; font-size: 16px; margin: 0; line-height: 1.6;">
                                    <b>Strategic Insight for {display_name}:</b><br>{advice}
                                </p>
                                <p style="color: #666; font-size: 14px; margin-top: 10px;">
                                    <b>Trend:</b> {trend.capitalize()} | <b>Confidence:</b> {confidence.capitalize()}
                                </p>
                            </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.warning("AI Market Advisor temporarily unavailable")

                except Exception as e:
                    st.error(f"Could not fetch AI market advice: {str(e)}")

                # =====================================================
                # DETAILED GAP ANALYSIS TABLE (WITH PAGINATION)
                # =====================================================
                
                # =====================================================
                # DETAILED GAP ANALYSIS TABLE (WITH PAGINATION)
                # =====================================================
                st.markdown("<br>", unsafe_allow_html=True)
                st.subheader(f"📊 Detailed Gap Analysis: {month_sel}")
                
                try:
                    # Pagination controls
                    gap_page = st.number_input("Page", min_value=1, value=1, step=1, key="gap_page")
                    gap_page_size = st.selectbox("Records per page", [10, 20, 50, 100], index=1, key="gap_page_size")
                    
                    gap_response = requests.get(
                        f"{BASE_URL}/gap-analysis", 
                        params={
                            "month": month_sel,
                            "page": gap_page,
                            "page_size": gap_page_size
                        }, 
                        headers=HEADERS, 
                        timeout=15
                    )
                    
                    if gap_response.status_code == 200:
                        gap_result = gap_response.json()
                        gap_data = gap_result.get('data', [])
                        pagination = gap_result.get('pagination', {})
                        
                        if gap_data:
                            # Convert to DataFrame
                            gap_df = pd.DataFrame(gap_data)
                            
                            # Normalize commodity names for display
                            gap_df['commodity'] = gap_df['commodity'].apply(normalize_commodity_for_display)
                            
                            # Rename columns for display
                            gap_display = gap_df.rename(columns={
                                'commodity': 'Commodity',
                                'min_price': 'Min Price',
                                'max_price': 'Max Price',
                                'avg_price': 'Avg Price',
                                'cheapest_source': 'Cheapest Source',
                                'top_selling_market': 'Top Selling Market'
                            })
                            
                            # Display table
                            st.dataframe(
                                gap_display.style.format({
                                    'Min Price': '₦{:,.2f}',
                                    'Max Price': '₦{:,.2f}',
                                    'Avg Price': '₦{:,.2f}'
                                }),
                                use_container_width=True,
                                hide_index=True
                            )
                            
                            # Pagination info
                            st.caption(f"Showing page {pagination.get('page', 1)} of {pagination.get('total_pages', 1)} | Total records: {pagination.get('total_records', 0)}")
                            
                            # Navigation buttons
                            col_prev, col_next = st.columns(2)
                            
                            with col_prev:
                                if pagination.get('has_previous', False):
                                    if st.button(" Previous Page", key="gap_prev"):
                                        st.rerun()
                            
                            with col_next:
                                if pagination.get('has_next', False):
                                    if st.button("Next Page ", key="gap_next"):
                                        st.rerun()
                        else:
                            st.info(f"No gap analysis data available for {month_sel}")
                    else:
                        st.warning("Gap analysis temporarily unavailable")
                
                except Exception as e:
                    st.warning(f"Gap analysis: {str(e)}")
                
                # =====================================================
                # MARKET COMPARISON SECTION (INTERNAL + EXTERNAL)
                # =====================================================
                st.markdown("<br>", unsafe_allow_html=True)
                st.subheader("📊 Market Comparison")
                st.write("Compare prices across internal and external markets for any commodity")

                try:
                    # --- BUILD COMBINED MARKET & COMMODITY POOL ---

                    # Internal markets from already-loaded all_data
                    internal_markets = sorted(all_data['market'].dropna().unique().tolist())

                    # Internal commodities (normalized for display)
                    internal_commodities_raw = all_data['commodity'].dropna().unique().tolist()
                    internal_commodities = sorted(set(normalize_commodity_for_display(c) for c in internal_commodities_raw))

                    # External markets & commodities from other-sources API
                    ext_markets = []
                    ext_commodities = []
                    ext_df_full = pd.DataFrame()
                    try:
                        ext_resp = requests.get(
                            f"{BASE_URL}/other-sources",
                            params={"page": 1, "page_size": 10000},
                            headers=HEADERS,
                            timeout=15
                        )
                        if ext_resp.status_code == 200:
                            ext_result = ext_resp.json()
                            ext_raw = ext_result.get('data', ext_result) if isinstance(ext_result, dict) else ext_result
                            if ext_raw:
                                ext_df_full = pd.DataFrame(ext_raw)
                                ext_df_full['date'] = pd.to_datetime(ext_df_full['date'], errors='coerce')
                                ext_df_full['month_name'] = ext_df_full['date'].dt.strftime('%B')
                                ext_df_full['price'] = pd.to_numeric(ext_df_full['price'], errors='coerce')
                                ext_markets = sorted(ext_df_full['location'].dropna().unique().tolist())
                                ext_commodities = sorted(ext_df_full['commodity'].dropna().unique().tolist())
                    except Exception:
                        pass

                    # Combined and deduplicated lists
                    all_combined_markets = sorted(set(
                        [f"[Internal] {m}" for m in internal_markets] +
                        [f"[External] {m}" for m in ext_markets]
                    ))
                    all_combined_commodities = sorted(set(internal_commodities + ext_commodities))

                    if len(all_combined_markets) >= 2 and all_combined_commodities:

                        # --- THREE DROPDOWNS ---
                        dd_col1, dd_col2, dd_col3 = st.columns(3)

                        with dd_col1:
                            comp_commodity = st.selectbox(
                                "🌾 Select Commodity",
                                all_combined_commodities,
                                index=all_combined_commodities.index(display_name) if display_name in all_combined_commodities else 0,
                                key="comp_commodity"
                            )

                        with dd_col2:
                            market_a = st.selectbox("Select First Market", all_combined_markets, key="comp_market_a")

                        with dd_col3:
                            remaining_markets = [m for m in all_combined_markets if m != market_a]
                            market_b = st.selectbox("Select Second Market", remaining_markets, key="comp_market_b")

                        # --- PRICE LOOKUP HELPER ---
                        def get_price_for_market(market_label, commodity_sel):
                            """
                            Returns avg price per kg for a given market label and commodity.
                            Internal markets use price_per_kg; external use price (assumed per kg unless unit=bag).
                            """
                            source_type = "internal" if market_label.startswith("[Internal]") else "external"
                            market_name = market_label.replace("[Internal] ", "").replace("[External] ", "")

                            if source_type == "internal":
                                # Match commodity: try exact normalized match first, then partial
                                api_name = convert_display_to_api_format(commodity_sel)
                                subset = all_data[all_data['market'].str.lower() == market_name.lower()]
                                subset = subset[subset['commodity'].str.lower() == api_name.lower()]
                                if subset.empty:
                                    subset = all_data[all_data['market'].str.lower() == market_name.lower()]
                                    subset = subset[subset['commodity'].str.contains(commodity_sel, case=False, na=False)]
                                if subset.empty:
                                    return None
                                col = 'price_per_kg' if target_col == 'price_per_kg' else 'price_per_bag'
                                val = pd.to_numeric(subset[col], errors='coerce').mean()
                                return round(float(val), 2) if pd.notna(val) else None

                            else:  # external
                                if ext_df_full.empty:
                                    return None
                                subset = ext_df_full[ext_df_full['location'].str.lower() == market_name.lower()]
                                subset = subset[subset['commodity'].str.contains(commodity_sel, case=False, na=False)]
                                if subset.empty:
                                    return None
                                # Normalize to per-kg if unit is bag (assume 100kg bag)
                                prices = []
                                for _, row in subset.iterrows():
                                    p = row['price']
                                    unit = str(row.get('unit', '')).lower()
                                    if 'bag' in unit:
                                        p = p / 100
                                    prices.append(p)
                                return round(float(pd.Series(prices).mean()), 2) if prices else None

                        # --- CALCULATE PRICES ---
                        price_a = get_price_for_market(market_a, comp_commodity)
                        price_b = get_price_for_market(market_b, comp_commodity)

                        unit_label_comp = "Avg/Kg" if price_choice == "Price per Kg" else "Avg/Bag"
                        source_a = "Internal" if market_a.startswith("[Internal]") else "External"
                        source_b = "Internal" if market_b.startswith("[Internal]") else "External"
                        name_a = market_a.replace("[Internal] ", "").replace("[External] ", "")
                        name_b = market_b.replace("[Internal] ", "").replace("[External] ", "")

                        if price_a is not None and price_b is not None:
                            diff = price_b - price_a
                            perc_diff = (diff / price_a) * 100 if price_a != 0 else 0

                            cheaper_name   = name_a if price_a <= price_b else name_b
                            cheaper_source = source_a if price_a <= price_b else source_b
                            cheaper_price  = min(price_a, price_b)
                            exp_name       = name_b if price_a <= price_b else name_a
                            exp_source     = source_b if price_a <= price_b else source_a
                            exp_price      = max(price_a, price_b)

                            # Display cards
                            comp_col1, comp_col2 = st.columns(2)
                            with comp_col1:
                                st.markdown(f"""
                                    <div class="strategy-card best-buy">
                                        <div style="font-size: 12px; opacity: 0.8; margin-bottom: 4px;">CHEAPER MARKET • {cheaper_source}</div>
                                        <div style="font-size: 24px; font-weight: bold; margin: 5px 0;">{cheaper_name}</div>
                                        <div style="font-size: 20px;">₦{cheaper_price:,.2f} <small>({unit_label_comp})</small></div>
                                    </div>
                                """, unsafe_allow_html=True)
                            with comp_col2:
                                st.markdown(f"""
                                    <div class="strategy-card worst-buy">
                                        <div style="font-size: 12px; opacity: 0.8; margin-bottom: 4px;">MORE EXPENSIVE • {exp_source}</div>
                                        <div style="font-size: 24px; font-weight: bold; margin: 5px 0;">{exp_name}</div>
                                        <div style="font-size: 20px;">₦{exp_price:,.2f} <small>({unit_label_comp})</small></div>
                                    </div>
                                """, unsafe_allow_html=True)

                            # Price difference insight
                            if abs(perc_diff) > 0:
                                direction = "more expensive" if perc_diff > 0 else "cheaper"
                                insight_text = (
                                    f"{name_b} ({source_b}) is **{abs(perc_diff):.1f}%** {direction} than "
                                    f"{name_a} ({source_a}) — **₦{abs(diff):,.2f}** difference"
                                )
                                st.markdown(f"""
                                    <div style="background-color: #FFF4E5; padding: 20px; border-radius: 10px;
                                                margin-top: 15px; border-left: 5px solid {ACCENT_COLOR};">
                                        <p style="color: #1F2937; font-size: 17px; font-weight: 600; margin: 0; line-height: 1.6;">
                                            💰 <b>Price Difference:</b> {insight_text}
                                        </p>
                                    </div>
                                """, unsafe_allow_html=True)

                        elif price_a is None and price_b is None:
                            st.warning(f"No price data found for **{comp_commodity}** in either selected market.")
                        elif price_a is None:
                            st.warning(f"No price data for **{comp_commodity}** in **{name_a}** ({source_a}).")
                        else:
                            st.warning(f"No price data for **{comp_commodity}** in **{name_b}** ({source_b}).")

                    else:
                        st.info("Not enough combined market data available to enable comparison.")

                except Exception as comp_err:
                    st.warning(f"Market comparison unavailable: {str(comp_err)}")
            else:
                st.info(f"Insufficient market data for strategic sourcing analysis of {display_name} in {month_sel}.")
        else:
            st.info(f"No data available for {month_sel}.")
except Exception as e:
    st.error(f"Monthly Report Error: {e}")

# =====================================================
# 10. OTHER SOURCES COMMODITY PRICES (WITH PROPER PAGINATION)
# =====================================================
st.markdown("---")
st.markdown("<h1 style='text-align:center; color: #1F7A3F;'>🌐 Externally Sourced Market Prices</h1>", unsafe_allow_html=True)

# Initialize page state for other sources
if "os_page" not in st.session_state:
    st.session_state.os_page = 1

try:
    # Pagination controls for Other Sources
    os_col1, os_col2 = st.columns(2)
    
    with os_col1:
        os_page_size = st.selectbox("Records per page", [100, 200, 500, 1000], index=0, key="os_page_size")
    
    with os_col2:
        st.write(f"Current Page: {st.session_state.os_page}")
    
    # Fetch other sources data with pagination using session state
    os_response = requests.get(
        f"{BASE_URL}/other-sources", 
        params={"page": st.session_state.os_page, "page_size": os_page_size},
        headers=HEADERS,
        timeout=15
    )
    
    if os_response.status_code == 200:
        os_result = os_response.json()
        
        # Handle both paginated and non-paginated responses
        if isinstance(os_result, dict) and 'data' in os_result:
            # Paginated response
            os_data_raw = os_result.get('data', [])
            os_pagination = os_result.get('pagination', {})
        elif isinstance(os_result, list):
            # Non-paginated response (old format)
            os_data_raw = os_result
            os_pagination = {
                "page": 1,
                "page_size": len(os_result),
                "total_records": len(os_result),
                "total_pages": 1,
                "has_next": False,
                "has_previous": False
            }
        else:
            os_data_raw = []
            os_pagination = {}
        
        if os_data_raw and len(os_data_raw) > 0:
            os_data = pd.DataFrame(os_data_raw)
            
            # Data processing
            os_data['date'] = pd.to_datetime(os_data['date'], errors='coerce')
            os_data['month_name'] = os_data['date'].dt.strftime('%B')
            os_data['year'] = os_data['date'].dt.year.astype(str)
            os_data['price'] = pd.to_numeric(os_data['price'], errors='coerce')
            
            # INDEPENDENT SIDEBAR FILTERS FOR OTHER SOURCES
            st.sidebar.markdown("---")
            st.sidebar.markdown("### 🌐 Externally Sourced Market Controls")
            
            # Get commodities from dedicated API endpoint
            try:
                filters_response = requests.get(
                    f"{BASE_URL}/filters/other-sources-commodities",
                    headers=HEADERS,
                    timeout=10
                )
                if filters_response.status_code == 200:
                    filters_data = filters_response.json()
                    os_commodities = ["All"] + filters_data['commodities']
                else:
                    os_commodities = ["All"] + sorted(os_data['commodity'].unique().tolist())
            except Exception as e:
                os_commodities = ["All"] + sorted(os_data['commodity'].unique().tolist())

            # Get locations from /filters/all endpoint
            try:
                loc_filters_response = requests.get(f"{BASE_URL}/filters/all", headers=HEADERS, timeout=10)
                if loc_filters_response.status_code == 200:
                    all_filters = loc_filters_response.json()
                    if 'other_sources' in all_filters:
                        os_locations = ["All"] + all_filters['other_sources']['locations']
                    else:
                        os_locations = ["All"] + sorted(os_data['location'].unique().tolist())
                else:
                    os_locations = ["All"] + sorted(os_data['location'].unique().tolist())
            except:
                os_locations = ["All"] + sorted(os_data['location'].unique().tolist())

            # Independent filters
            selected_os_comm = st.sidebar.selectbox(
                "Other sources Commodity", 
                os_commodities, 
                key="os_comm_independent"
            )

            selected_os_loc = st.sidebar.selectbox(
                "Other sources Market", 
                os_locations, 
                key="os_loc_independent"
            )
            
            os_months = ["All", "January", "February", "March", "April", "May", "June", 
                        "July", "August", "September", "October", "November", "December"]
            selected_os_month = st.sidebar.selectbox(
                "Other sources Month", 
                os_months, 
                key="os_month_independent"
            )
            
            # Apply filters
            filtered_os = os_data.copy()
            
            # Filter by selected years from main sidebar
            if 'selected_years' in locals() and selected_years and len(selected_years) > 0:
                if 'year' in filtered_os.columns and not filtered_os['year'].isna().all():
                    filtered_os = filtered_os[filtered_os['year'].isin(selected_years)]
            
            # Apply other sources specific filters
            if selected_os_comm != "All":
                filtered_os = filtered_os[filtered_os['commodity'] == selected_os_comm]
            
            if selected_os_loc != "All":
                filtered_os = filtered_os[filtered_os['location'] == selected_os_loc]
            
            if selected_os_month != "All":
                filtered_os = filtered_os[filtered_os['month_name'] == selected_os_month]
            
            # Search box
            os_search = st.text_input(
                "🔍 Search by commodity, location...", 
                placeholder="Enter keyword...",
                key="os_search_input"
            )
            
            if os_search:
                mask = filtered_os.apply(
                    lambda row: row.astype(str).str.contains(os_search, case=False).any(), 
                    axis=1
                )
                filtered_os = filtered_os[mask]
            
            # Display table
            if not filtered_os.empty:
                # Format the display dataframe
                display_df = filtered_os[['date', 'commodity', 'location', 'unit', 'price']].copy()
                
                # Format date column
                display_df['Date'] = display_df['date'].dt.strftime('%d %b %Y, %I:%M %p')
                
                # Rename columns
                display_df = display_df.rename(columns={
                    'commodity': 'Commodity',
                    'location': 'Location',
                    'unit': 'unit',
                    'price': 'Price (₦)'
                })
                
                # Select final columns in correct order
                display_df = display_df[['Date', 'Commodity', 'Location', 'unit', 'Price (₦)']]
                
                # Display with formatting
                st.dataframe(
                    display_df.style.format({
                        'Price (₦)': '₦{:,.0f}'
                    }),
                    use_container_width=True,
                    hide_index=True,
                    height=600
                )

                # Pagination info and navigation
                st.markdown("---")
                
                # Create columns for pagination controls
                download_col1, download_col2 = st.columns([1, 2])

                with download_col1:
                    # PDF Download Button (keeping existing functionality)
                    if st.button("📄 Download Monthly Report (PDF)", key="download_os_pdf"):
                        try:
                            # Create PDF buffer
                            pdf_buffer = BytesIO()
                            doc = SimpleDocTemplate(pdf_buffer, pagesize=letter)
                            elements = []
                            styles = getSampleStyleSheet()
                            
                            # Title
                            title = Paragraph(f"<b>Externally Sourced Market Prices</b>", styles['Title'])
                            elements.append(title)
                            elements.append(Spacer(1, 12))
                            
                            # Date and summary
                            date_text = f"Report Date: {datetime.now().strftime('%B %d, %Y')}"
                            date_para = Paragraph(date_text, styles['Normal'])
                            elements.append(date_para)
                            
                            summary_text = f"Total Records: {len(filtered_os):,}"
                            summary_para = Paragraph(summary_text, styles['Normal'])
                            elements.append(summary_para)
                            elements.append(Spacer(1, 12))
                            
                            # Prepare table data
                            table_data = [['Date', 'Commodity', 'Location', 'Price', 'Unit']]
                            
                            for _, row in filtered_os.head(500).iterrows():
                                table_data.append([
                                    str(row['date'])[:10],
                                    str(row['commodity']),
                                    str(row['location'])[:30],
                                    f"N{int(row['price']):,}",
                                    str(row['unit'])
                                ])
                            
                            # Create table
                            t = Table(table_data)
                            t.setStyle(TableStyle([
                                ('BACKGROUND', (0, 0), (-1, 0), colors.green),
                                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                                ('FONTSIZE', (0, 0), (-1, 0), 10),
                                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                                ('FONTSIZE', (0, 1), (-1, -1), 8),
                            ]))
                            
                            elements.append(t)
                            
                            # Build PDF
                            doc.build(elements)
                            pdf_buffer.seek(0)
                            
                            # Download button
                            st.download_button(
                                label="⬇️ Click to Download",
                                data=pdf_buffer,
                                file_name=f"Other_Sources_{datetime.now().strftime('%Y%m%d')}.pdf",
                                mime="application/pdf",
                                key="download_pdf_file"
                            )
                            
                            st.success("✅ PDF generated successfully!")
                            
                        except Exception as e:
                            st.error(f"Failed to generate PDF: {e}")

                with download_col2:
                    # Pagination info display
                    st.markdown(
                        f"""
                        <div style='text-align: right; padding: 10px; color: #666; font-size: 14px;'>
                            📊 Showing {len(display_df)} records 
                            (Page {os_pagination.get('page', 1)} of {os_pagination.get('total_pages', 1)} | 
                            Total: {os_pagination.get('total_records', len(os_data)):,})
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
                
                # Pagination navigation with session state
                os_nav_col1, os_nav_col2, os_nav_col3 = st.columns([1, 2, 1])
                
                with os_nav_col1:
                    if os_pagination.get('has_previous', False):
                        if st.button("⬅ Previous", key="os_prev"):
                            st.session_state.os_page -= 1
                            st.rerun()
                
                with os_nav_col2:
                    st.write(f"Page {os_pagination.get('page',1)} of {os_pagination.get('total_pages',1)}")
                
                with os_nav_col3:
                    if os_pagination.get('has_next', False):
                        if st.button("Next ➡", key="os_next"):
                            st.session_state.os_page += 1
                            st.rerun()
            else:
                st.warning("⚠️ No data matches your filter criteria.")
                
                # Show helpful debugging info
                if 'selected_years' in locals() and selected_years:
                    available_years = sorted(os_data['year'].unique().tolist()) if 'year' in os_data.columns else []
                    st.info(f"""
                    **Filter Issue:**
                    - Years selected in sidebar: {', '.join(selected_years)}
                    - Years available in Other Sources data: {', '.join(available_years)}
                    
                    **Tip:** Adjust the Year filter in the main sidebar to match your data.
                    """)
                else:
                    st.info("Try changing the filters in the sidebar (Commodity, Market, Month, Year).")
        else:
            st.info("📭 **No Other Sources data available yet.**")
            st.write("""
            **To add data to this section:**
            1. Use the `upload_other_sources_smart.py` script to upload scraped market data
            2. Or use the API endpoint: `POST /bulk-upload-other-sources`
            
            **Expected data format:**
            - date
            - commodity  
            - location (market name)
            - unit
            - price
            
            Once you upload data, it will appear here automatically!
            """)
    else:
        st.error(f"❌ Failed to fetch Other Sources data. API Status: {os_response.status_code}")
        st.write("Please check if the backend API is running correctly.")
        
except Exception as e:
    st.error(f"Other Sources Error: {e}")
# =====================================================
# 11. FOOTER
# =====================================================
st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #666; padding: 20px;'>
        <p><strong>Agriarche Intelligence Hub</strong> — Agricultural Market Intelligence Platform</p>
        <p style='font-size: 0.9em;'> • Real-time commodity pricing data</p>
    </div>
""", unsafe_allow_html=True)
