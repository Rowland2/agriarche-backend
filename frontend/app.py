import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# UI CONFIG
st.set_page_config(page_title="Agriarche Dashboard", layout="wide")

# CUSTOM CSS FOR MOBILE HARMONIZATION
st.markdown("""
    <style>
    @media (max-width: 640px) {
        .stHorizontalBlock { flex-direction: column !important; }
        [data-testid="stMetricValue"] { font-size: 1.5rem !important; }
    }
    .metric-card {
        background: white; padding: 15px; border-radius: 10px;
        border-left: 5px solid #1F7A3F; box-shadow: 2px 2px 5px rgba(0,0,0,0.05);
    }
    </style>
""", unsafe_allow_html=True)

# API ENDPOINT (Change this once hosted on Render)
API_URL = "http://127.0.0.1:8000" 

st.title("🌾 Agriarche Intelligence Hub")

# SIDEBAR
comm = st.sidebar.selectbox("Commodity", ["Maize", "Soybeans", "Sorghum"])
month = st.sidebar.selectbox("Month", ["January", "February", "March"])

# FETCH DATA FROM API (Instead of local processing)
try:
    # Get Intel
    intel_res = requests.get(f"{API_URL}/intelligence/{comm}").json()
    # Get Analysis
    analysis_res = requests.get(f"{API_URL}/analysis", params={"commodity": comm, "month": month}).json()
    
    metrics = analysis_res.get("metrics")
    
    if metrics:
        col1, col2, col3 = st.columns(3)
        col1.metric("Avg Price", f"₦{metrics['avg']:,.2f}")
        col2.metric("Max Price", f"₦{metrics['max']:,.2f}")
        col3.metric("Min Price", f"₦{metrics['min']:,.2f}")
        
        st.info(f"**Intelligence:** {intel_res['info']['desc']}")
    else:
        st.warning("No data returned from API for this selection.")

except Exception as e:
    st.error(f"Could not connect to Backend API. Is it running? Error: {e}")