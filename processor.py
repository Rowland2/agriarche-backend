import pandas as pd

# Master Data for Intelligence
COMMODITY_INFO = {
    "Soybeans": {"desc": "A raw leguminous crop used for oil and feed.", "markets": "Mubi, Giwa, and Kumo", "abundance": "Nov, Dec, and April", "note": "A key industrial driver for the poultry and vegetable oil sectors."},
    "Cowpea Brown": {"desc": "Protein-rich legume popular in local diets.", "markets": "Dawanau and Potiskum", "abundance": "Oct through Jan", "note": "Supply depends on Northern storage."},
    "Cowpea White": {"desc": "Staple bean variety used for commercial flour.", "markets": "Dawanau and Bodija", "abundance": "Oct and Nov", "note": "High demand in South drives prices."},
    "Honey beans": {"desc": "Premium sweet brown beans (Oloyin).", "markets": "Oyingbo and Dawanau", "abundance": "Oct to Dec", "note": "Often carries a price premium."},
    "Maize": {"desc": "Primary cereal crop for food and industry.", "markets": "Giwa, Makarfi, and Funtua", "abundance": "Sept to Nov", "note": "Correlates strongly with Sorghum trends."},
    "Rice Paddy": {"desc": "Raw rice before milling/processing.", "markets": "Argungu and Kano", "abundance": "Nov and Dec", "note": "Foundations for processed rice pricing."},
    "Rice processed": {"desc": "Milled and polished local rice.", "markets": "Kano, Lagos, and Onitsha", "abundance": "Year-round", "note": "Price fluctuates with fuel/milling costs."},
    "Sorghum": {"desc": "Drought-resistant grain staple.", "markets": "Dawanau and Gombe", "abundance": "Dec and Jan", "note": "Market substitute for Maize."},
    "Millet": {"desc": "Fast-growing cereal for the lean season.", "markets": "Dawanau and Potiskum", "abundance": "Sept and Oct", "note": "First harvest after rainy season."},
    "Groundnut gargaja": {"desc": "Local peanut variety for oil extraction.", "markets": "Dawanau and Gombe", "abundance": "Oct and Nov", "note": "Sahel region specialty."},
    "Groundnut kampala": {"desc": "Large, premium roasting groundnuts.", "markets": "Kano and Dawanau", "abundance": "Oct and Nov", "note": "Higher oil content than Gargaja."}
}

def normalize_name(text):
    text = str(text).lower().strip()
    if "soya" in text or "soy" in text: return "Soybeans"
    if "maize" in text or "corn" in text: return "Maize"
    if "cowpea" in text and "brown" in text: return "Cowpea Brown"
    if "cowpea" in text and "white" in text: return "Cowpea White"
    if "honey" in text: return "Honey beans"
    if "rice" in text and "paddy" in text: return "Rice Paddy"
    if "rice" in text and "process" in text: return "Rice processed"
    if "sorghum" in text: return "Sorghum"
    if "groundnut" in text and "gargaja" in text: return "Groundnut gargaja"
    if "groundnut" in text and "kampala" in text: return "Groundnut kampala"
    return text.capitalize()

def calculate_metrics(df, commodity, month, market=None):
    """Business logic for KPI calculations"""
    dfc = df[(df["commodity"] == commodity) & (df["month_name"] == month)]
    if market and market != "All Markets":
        dfc = dfc[dfc["Market"] == market]
    
    if dfc.empty: return None
    
    return {
        "avg": dfc["price_per_kg"].mean(),
        "max": dfc["price_per_kg"].max(),
        "min": dfc["price_per_kg"].min(),
        "count": len(dfc)
    }

def get_strategic_advice(avg_p, min_p, max_p, annual_avg, commodity, month, best_m):
    volatility = ((max_p - min_p) / min_p) * 100 if min_p > 0 else 0
    if volatility > 20:
        advice = f"🚨 High Volatility Warning: Prices are fluctuating ({volatility:.1f}%). Avoid spot-buying in {month}."
    elif avg_p < annual_avg:
        advice = f"✅ Optimal Buy Window: Prices are {((annual_avg-avg_p)/annual_avg)*100:.1f}% below annual average."
    else:
        advice = f"ℹ️ Market Stability: Prioritize procurement in {best_m}."
    return advice