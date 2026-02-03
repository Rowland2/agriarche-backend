from fastapi import FastAPI, Security, HTTPException, status, Depends
from fastapi.security.api_key import APIKeyHeader
import sqlite3
import pandas as pd
import os
from processor import normalize_name, calculate_metrics, COMMODITY_INFO

app = FastAPI(title="Agriarche Data Hub")

API_KEY = "Agriarche_Internal_Key_2026"
api_key_header = APIKeyHeader(name="access_token")

@app.get("/")
def home():
    return {"status": "Agriarche API is online", "database": "Connected"}

@app.get("/intelligence/{commodity}")
def get_intelligence(commodity: str):
    info = COMMODITY_INFO.get(commodity)
    if not info:
        return {"info": {"desc": "Market data pending for this commodity.", "markets": "N/A"}}
    return {"info": info}

def fetch_data():
    # Priority: current folder, then parent folder
    db_path = 'kasuwa.db' if os.path.exists('kasuwa.db') else '../kasuwa.db'
    try:
        conn = sqlite3.connect(db_path)
        # We use pd.read_sql but ensure we handle potential missing tables
        df = pd.read_sql("SELECT * FROM prices", conn)
        conn.close()
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analysis")
def full_analysis(commodity: str, month: str, years: str = "", market: str = "All Markets"):
    df = fetch_data()
    
    # 1. Standardize column types
    df['Start Time'] = pd.to_datetime(df['Start Time'])
    df['month_name'] = df['Start Time'].dt.strftime('%B')
    df['year_str'] = df['Start Time'].dt.year.astype(str)
    
    # 2. Filtering using EXACT capitalization from Screenshot (1083)
    df = df[df['Commodity'] == commodity]
    df = df[df['month_name'] == month]
    
    if market != "All Markets":
        df = df[df['Market'] == market]
    
    if years:
        selected_years = years.split(',')
        df = df[df['year_str'].isin(selected_years)]

    # 3. SAFETY CHECK: If no data found, return empty results instead of crashing
    if df.empty:
        return {
            "chart_data": [],
            "metrics": {"avg": 0, "max": 0, "min": 0}
        }

    # 4. Success: Prepare data for the orange Plotly chart
    result = df[['Market', 'price_per_kg', 'Price per Bag', 'Start Time']].to_dict(orient='records')
    
    return {
        "chart_data": result,
        "metrics": {
            "avg": round(float(df['price_per_kg'].mean()), 2),
            "max": float(df['price_per_kg'].max()),
            "min": float(df['price_per_kg'].min())
        }
    }

@app.post("/update-price")
def update_price(data: dict, token: str = Depends(api_key_header)):
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    db_path = 'kasuwa.db' if os.path.exists('kasuwa.db') else '../kasuwa.db'
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Adjusted columns to match your Excel-to-DB move
        cursor.execute("""
            INSERT INTO prices ("Start Time", "Market", "Commodity", "Price per Bag", "price_per_kg")
            VALUES (?, ?, ?, ?, ?)
        """, (data['date'], data['market'], data['commodity'], data['price_bag'], data['price_kg']))
        conn.commit()
        conn.close()
        return {"status": "success", "message": f"Added record for {data['commodity']}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")
