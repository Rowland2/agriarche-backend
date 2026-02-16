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
    "Soya Beans": {"desc": "A raw leguminous crop used for oil and feed.", "markets": "Mubi, Giwa, and Kumo", "abundance": "Nov, Dec, and April", "note": "A key industrial driver for the poultry and vegetable oil sectors."},
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
        return "Soya Beans"
    elif "honey" in name_lower:
        return "Honey beans"
    elif "rice" in name_lower and "paddy" in name_lower:
        return "Rice Paddy"
    elif "rice" in name_lower and "process" in name_lower:
        return "Processed Rice"
    elif "millet" in name_lower:
        return "Millet"
    elif "groundnut" in name_lower and "gargaja" in name_lower:
        return "Groundnut gargaja"
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
        "Soya Beans": "Soya Beans",
        "Honey beans": "Honey beans",
        "Rice Paddy": "Rice Paddy",
        "Processed Rice": "Rice processed",
        "Millet": "Millet",
        "Groundnut gargaja": "Groundnut gargaja",
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
# 5. SIDEBAR
# =====================================================
st.sidebar.title("Internal Market Price Filters")
commodity_raw = st.sidebar.selectbox("Select Commodity", HARDCODED_COMMODITIES)
market_sel = st.sidebar.selectbox("Select Market", ["All Markets"] + HARDCODED_MARKETS)
month_sel = st.sidebar.selectbox("Select Month", ["January", "February", "March", "April", "May", "June", 
                                                   "July", "August", "September", "October", "November", "December"])

# Add Year filter to main sidebar (moved from Other sources)
years_list = ["2024", "2025", "2026"]
selected_years = st.sidebar.multiselect("Year", years_list, default=["2026"], key="main_years")

price_choice = st.sidebar.radio("Display Price By:", ["Price per Kg", "Price per Bag"])

display_name = format_commodity_name(commodity_raw)
target_col = "price_per_kg" if price_choice == "Price per Kg" else "price_per_bag"

# Convert display name back to API format for querying
api_commodity_name = convert_display_to_api_format(commodity_raw)

# =====================================================
# 6. MAIN CONTENT (CHART & KPIs)
# =====================================================
# Display logo at top
try:
    import os
    # Try to load logo - handle both local and deployed environments
    if os.path.exists(LOGO_PATH):
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.image(LOGO_PATH, use_container_width=True)
    else:
        # Fallback: Show Agriarche text logo
        st.markdown("""
            <div style='text-align: center; padding: 20px;'>
                <h1 style='color: #1F7A3F; font-size: 48px; font-weight: bold; margin: 0;'>
                    🌾 AGRIARCHE
                </h1>
            </div>
        """, unsafe_allow_html=True)
except Exception as e:
    # If any error loading logo, show text fallback
    st.markdown("""
        <div style='text-align: center; padding: 20px;'>
            <h1 style='color: #1F7A3F; font-size: 48px; font-weight: bold; margin: 0;'>
                🌾 AGRIARCHE
            </h1>
        </div>
    """, unsafe_allow_html=True)

st.title("Commodity Pricing Intelligence Dashboard")
st.subheader(f"Market Price Trend: {display_name} in {month_sel}")

# A. Chart Fetch (Filtered)
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
                    dtick=1  # Force whole number intervals (1, 2, 3, not 1.2, 1.4)
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
            # If not found, try normalized version
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
            
            # =====================================================
            # STRATEGIC SOURCING - MOVED HERE (after Commodity Intelligence)
            # =====================================================
            # Get full month data for strategic sourcing
            try:
                all_response = requests.get(f"{BASE_URL}/prices", headers=HEADERS)
                if all_response.status_code == 200:
                    all_data = pd.DataFrame(all_response.json())
                    all_data['start_time'] = pd.to_datetime(all_data['start_time'])
                    all_data['month_name'] = all_data['start_time'].dt.strftime('%B')
                    all_data['price_per_kg'] = pd.to_numeric(all_data['price_per_kg'], errors='coerce')
                    all_data['price_per_bag'] = pd.to_numeric(all_data['price_per_bag'], errors='coerce')
                    
                    # Filter for selected month and commodity
                    report_data = all_data[all_data['month_name'] == month_sel].copy()
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
        
        else:
            st.warning(f"No chart data found for {display_name} in {month_sel}.")
except Exception as e:
    st.error(f"Chart Error: {e}")

# =====================================================
# 7. STANDALONE DATA ARCHIVE TABLE
# =====================================================
st.markdown("---")
st.subheader("📚 Internal Market Price Data Archive")
# st.write("Search through all price records regardless of sidebar filters.")

try:
    full_res = requests.get(f"{BASE_URL}/prices", headers=HEADERS)
    if full_res.status_code == 200:
        all_raw_data = full_res.json()
        
        if all_raw_data:
            df_hist = pd.DataFrame(all_raw_data)
            hist_search = st.text_input("🔍 Search Price Records", placeholder="Search by market, year, or commodity...", key="hist_search_bar")
            
            # Formatting and cleaning columns
            df_hist["Date"] = pd.to_datetime(df_hist["start_time"])
            df_hist["Price per Kg (₦)"] = pd.to_numeric(df_hist["price_per_kg"], errors='coerce')
            df_hist["Price per Bag (₦)"] = pd.to_numeric(df_hist["price_per_bag"], errors='coerce')
            
            # Apply commodity name normalization (Color First format)
            df_hist["commodity"] = df_hist["commodity"].apply(normalize_commodity_for_display)
            
            # Sort by date, commodity, and market to calculate price changes
            df_hist = df_hist.sort_values(['commodity', 'market', 'Date'])
            
            # Calculate Old Price (previous price for same commodity + market)
            df_hist['Old Price (₦)'] = df_hist.groupby(['commodity', 'market'])['Price per Kg (₦)'].shift(1)
            
            # Calculate % Change
            df_hist['% Change'] = ((df_hist['Price per Kg (₦)'] - df_hist['Old Price (₦)']) / df_hist['Old Price (₦)'] * 100).round(2)
            
            # Format date for display
            df_hist["Date_Display"] = df_hist["Date"].dt.strftime('%Y-%m-%d')

            # Selecting columns to display - COMPACT VIEW
            display_cols = ["Date_Display", "commodity", "market", "Old Price (₦)", "Current Price per Kg (₦)", "% Change"]
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
            
            # Display sorted by newest date first
            st.dataframe(
                hist_display.sort_values(by="Date", ascending=False).style.format({
                    "Old Price (₦)": lambda x: f"{x:,.2f}" if pd.notna(x) else "—",
                    "Price per Kg (₦)": "{:,.2f}",
                    "% Change": lambda x: f"{x:+.2f}%" if pd.notna(x) else "—"
                }),
                use_container_width=True,
                hide_index=True,
                height=400
            )
        else:
            st.info("No records available in the database archive.")
except Exception as e:
    st.error(f"Archive Table Error: {e}")

# =====================================================
# 8. MONTHLY INTELLIGENCE REPORT & STRATEGIC SOURCING
# =====================================================
st.markdown("---")
st.header(f"📋 Monthly Intelligence Report: {month_sel}")

# Fetch month-specific data for PDF and analysis
try:
    month_response = requests.get(f"{BASE_URL}/prices", headers=HEADERS)
    if month_response.status_code == 200:
        all_data = pd.DataFrame(month_response.json())
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
                st.markdown("<br>", unsafe_allow_html=True)
                st.subheader("🤖 AI Market Advisor")
                
                volatility = ((max_val - min_val) / min_val) * 100 if min_val > 0 else 0
                annual_avg = all_data[all_data["commodity"].str.lower() == api_commodity_name.lower()]["price_per_kg"].mean()
                
                if volatility > 20:
                    advice = f"🚨 **High Volatility Warning:** {display_name} prices are fluctuating significantly ({volatility:.1f}%). Avoid spot-buying; look for long-term fixed contracts in {best_m}."
                    bg_adv = "#FFF4E5"
                elif avg_val < annual_avg:
                    advice = f"✅ **Optimal Buy Window:** Prices for {display_name} in {month_sel} are {((annual_avg-avg_val)/annual_avg)*100:.1f}% below the annual average. Strong window for inventory stocking."
                    bg_adv = "#E8F5E9"
                else:
                    advice = f"ℹ️ **Market Stability:** {display_name} is showing stable price action. Proceed with standard procurement volumes, prioritizing {best_m} for the best margins."
                    bg_adv = "#E3F2FD"

                st.markdown(f"""
                    <div class="advisor-container" style="background-color: {bg_adv};">
                        <p style="color: #1F2937; font-size: 16px; margin: 0; line-height: 1.6;">
                            <b>Strategic Insight for {display_name}:</b><br>{advice}
                        </p>
                    </div>
                """, unsafe_allow_html=True)
                
                # =====================================================
                # MARKET COMPARISON SECTION
                # =====================================================
                st.markdown("<br>", unsafe_allow_html=True)
                st.subheader("📊 Market Comparison")
                st.write(f"Compare {display_name} prices across different markets for {month_sel}")
                
                # Show which markets have data for this commodity
                markets_with_data = sorted(strategy_df['market'].unique().tolist())
                
                if len(markets_with_data) >= 2:
                    st.info(f"📍 Markets with {display_name} data in {month_sel}: {', '.join(markets_with_data)}")
                    
                    col_comp1, col_comp2 = st.columns(2)
                    
                    with col_comp1:
                        market_a = st.selectbox("Select First Market", markets_with_data, key="comp_market_a")
                    
                    with col_comp2:
                        remaining_markets = [m for m in markets_with_data if m != market_a]
                        market_b = st.selectbox("Select Second Market", remaining_markets, key="comp_market_b")
                    
                    # Calculate prices for both markets
                    price_a = strategy_df[strategy_df['market'] == market_a][target_col].mean()
                    price_b = strategy_df[strategy_df['market'] == market_b][target_col].mean()
                    
                    # Calculate difference
                    diff = price_b - price_a
                    perc_diff = (diff / price_a) * 100 if price_a != 0 else 0
                    
                    # Determine which is cheaper
                    cheaper_market = market_a if price_a < price_b else market_b
                    cheaper_price = min(price_a, price_b)
                    expensive_market = market_b if price_a < price_b else market_a
                    expensive_price = max(price_a, price_b)
                    
                    unit_label_comp = "Avg/Kg" if price_choice == "Price per Kg" else "Avg/Bag"
                    
                    # Display comparison cards
                    comp_col1, comp_col2 = st.columns(2)
                    
                    with comp_col1:
                        st.markdown(f"""
                            <div class="strategy-card best-buy">
                                <div style="font-size: 14px; opacity: 0.9;">CHEAPER MARKET</div>
                                <div style="font-size: 24px; font-weight: bold; margin: 5px 0;">{cheaper_market}</div>
                                <div style="font-size: 20px;">₦{cheaper_price:,.2f} <small>({unit_label_comp})</small></div>
                            </div>
                        """, unsafe_allow_html=True)
                    
                    with comp_col2:
                        st.markdown(f"""
                            <div class="strategy-card worst-buy">
                                <div style="font-size: 14px; opacity: 0.9;">MORE EXPENSIVE</div>
                                <div style="font-size: 24px; font-weight: bold; margin: 5px 0;">{expensive_market}</div>
                                <div style="font-size: 20px;">₦{expensive_price:,.2f} <small>({unit_label_comp})</small></div>
                            </div>
                        """, unsafe_allow_html=True)
                    
                    # Price difference insight - MORE READABLE
                    if abs(perc_diff) > 0:
                        insight_text = f"{market_b} is **{abs(perc_diff):.1f}%** {'more expensive' if perc_diff > 0 else 'cheaper'} than {market_a} (**₦{abs(diff):,.2f}** difference)"
                        
                        st.markdown(f"""
                            <div style="background-color: #FFF4E5; padding: 20px; border-radius: 10px; margin-top: 15px; border-left: 5px solid {ACCENT_COLOR};">
                                <p style="color: #1F2937; font-size: 17px; font-weight: 600; margin: 0; line-height: 1.6;">
                                    💰 <b>Price Difference:</b> {insight_text}
                                </p>
                            </div>
                        """, unsafe_allow_html=True)
                
                else:
                    st.info(f"Need at least 2 markets with data for {display_name} to enable comparison.")
            else:
                st.info(f"Insufficient market data for strategic sourcing analysis of {display_name} in {month_sel}.")
        else:
            st.info(f"No data available for {month_sel}.")
except Exception as e:
    st.error(f"Monthly Report Error: {e}")

# =====================================================
# 10. OTHER SOURCES COMMODITY PRICES (MOVED TO END)
# =====================================================
st.markdown("---")
st.markdown("<h1 style='text-align:center; color: #1F7A3F;'>🌐 Externally Sourced Market Prices</h1>", unsafe_allow_html=True)

try:
    # Fetch other sources data
    os_response = requests.get(f"{BASE_URL}/other-sources", headers=HEADERS)
    
    if os_response.status_code == 200:
        os_data_raw = os_response.json()
        
        if os_data_raw:
            os_data = pd.DataFrame(os_data_raw)
            
            # Data processing
            os_data['date'] = pd.to_datetime(os_data['date'], errors='coerce')
            os_data['month_name'] = os_data['date'].dt.strftime('%B')
            os_data['year'] = os_data['date'].dt.year.astype(str)
            os_data['price'] = pd.to_numeric(os_data['price'], errors='coerce')
            
            # INDEPENDENT SIDEBAR FILTERS FOR OTHER SOURCES
            st.sidebar.markdown("---")
            st.sidebar.markdown("### 🌐 Externally Sourced Market Controls")
            
            # Independent Commodity filter for Other sources
            os_commodities = ["All"] + sorted(os_data['commodity'].unique().tolist())
            selected_os_comm = st.sidebar.selectbox(
                "Other sources Commodity", 
                os_commodities, 
                key="os_comm_independent"
            )
            
            # Independent Market filter for Other sources
            os_locations = ["All"] + sorted(os_data['location'].unique().tolist())
            selected_os_loc = st.sidebar.selectbox(
                "Other sources Market", 
                os_locations, 
                key="os_loc_independent"
            )
            
            # Independent Month filter for Other sources
            os_months = ["All", "January", "February", "March", "April", "May", "June", 
                        "July", "August", "September", "October", "November", "December"]
            selected_os_month = st.sidebar.selectbox(
                "Other sources Month", 
                os_months, 
                key="os_month_independent"
            )
            
            # Apply INDEPENDENT filters (don't use main filters)
            filtered_os = os_data.copy()
            
            # Filter by selected years from MAIN sidebar (Year filter is shared)
            if selected_years:
                filtered_os = filtered_os[filtered_os['year'].isin(selected_years)]
            
            # Apply Other sources specific filters
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
            
            # Display table - EXACT MATCH TO SCREENSHOT
            if not filtered_os.empty:
                # Format the display dataframe
                display_df = filtered_os[['date', 'commodity', 'location', 'unit', 'price']].copy()
                
                # Format date column to match screenshot
                display_df['Date'] = display_df['date'].dt.strftime('%d %b %Y, %I:%M %p')
                
                # Rename columns to match screenshot
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
                
                # Show record count with better visibility
                st.markdown(f"""
                    <div style="background-color: #E8F5E9; padding: 15px; border-radius: 8px; margin-top: 10px; text-align: center;">
                        <p style="color: #1F7A3F; font-size: 18px; font-weight: bold; margin: 0;">
                            📊 Showing {len(display_df):,} records from Other sources
                        </p>
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.warning("No data matches your filter criteria.")
        else:
            st.info("📭 No other sources data available yet. Upload your scraped data using the upload script.")
    else:
        st.error(f"Could not fetch other sources data. API Status: {os_response.status_code}")
        
except Exception as e:
    st.error(f"Other Sources Error: {e}")

# =====================================================
# 11. FOOTER
# =====================================================
st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #666; padding: 20px;'>
        <p><strong>Agriarche Intelligence Hub</strong> — Agricultural Market Intelligence Platform</p>
        <p style='font-size: 0.9em;'> Real-time commodity pricing data</p>
    </div>
""", unsafe_allow_html=True)