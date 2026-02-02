from fastapi import FastAPI, Security, HTTPException, status, Depends
from fastapi.security.api_key import APIKeyHeader
import sqlite3
import pandas as pd
import os
# Ensure COMMODITY_INFO is imported from processor
from processor import normalize_name, calculate_metrics, COMMODITY_INFO

app = FastAPI(title="Agriarche Data Hub")

API_KEY = "Agriarche_Internal_Key_2026"
api_key_header = APIKeyHeader(name="access_token")

# 1. Home Route
@app.get("/")
def home():
    return {"status": "Agriarche API is online", "database": "Connected"}

# 2. Intelligence Route
@app.get("/intelligence/{commodity}")
def get_intelligence(commodity: str):
    info = COMMODITY_INFO.get(commodity)
    if not info:
        return {"info": {"desc": "Market data pending for this commodity.", "markets": "N/A"}}
    return {"info": info}

# 3. Database Fetcher
def fetch_data():
    # Looks for db in current folder or parent folder
    db_path = 'kasuwa.db' if os.path.exists('kasuwa.db') else '../kasuwa.db'
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql("SELECT * FROM prices", conn)
        conn.close()
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# 4. Analysis Route (Fixed to return chart data and metrics)
@app.get("/analysis")
def full_analysis(
    commodity: str,
    month: str,
    years: str = "",
    market: str = "All Markets"
):
    df = fetch_data()

    year_list = [int(y) for y in years.split(",") if y.isdigit()] if years else None

    return calculate_metrics(df, commodity, month, year_list, market)
