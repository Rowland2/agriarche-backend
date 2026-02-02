from fastapi import FastAPI
import pandas as pd
from processor import normalize_name, calculate_metrics, COMMODITY_INFO

app = FastAPI(title="Agriarche Commodity API")

# In a real scenario, this would load from Google Sheets
# For now, it's a placeholder for the tech team to connect
def fetch_data():
    # Looks one level up for the Excel file
    return pd.read_excel("../Predictive Analysis Commodity pricing.xlsx")

@app.get("/")
def home():
    return {"message": "Agriarche Data API is Live"}

@app.get("/intelligence/{commodity}")
def get_intelligence(commodity: str):
    norm_name = normalize_name(commodity)
    info = COMMODITY_INFO.get(norm_name, {"error": "Not found"})
    return {"commodity": norm_name, "info": info}

@app.get("/analysis")
def full_analysis(commodity: str, month: str):
    # This endpoint would be called by Streamlit
    df = fetch_data()
    # Apply normalization to the whole DF first
    df["commodity"] = df["commodity"].apply(normalize_name)
    metrics = calculate_metrics(df, commodity, month)
    return {"metrics": metrics}