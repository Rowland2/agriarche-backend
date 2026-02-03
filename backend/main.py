from fastapi import FastAPI, Security, HTTPException, status, Depends
from fastapi.security.api_key import APIKeyHeader
import sqlite3
import pandas as pd
import os
from processor import normalize_name, calculate_metrics, COMMODITY_INFO

app = FastAPI(title="Agriarche Data Hub")

API_KEY = "Agriarche_Internal_Key_2026"
api_key_header = APIKeyHeader(name="access_token")

# HELPER: Path Finder to ensure we find the DB on Render
def get_db_path():
    paths = ['kasuwa.db', 'backend/kasuwa.db', '../kasuwa.db']
    for path in paths:
        if os.path.exists(path):
            return path
    return None

@app.get("/")
def home():
    path = get_db_path()
    return {
        "status": "Agriarche API is online", 
        "database_file_found": path is not None,
        "path_found": path,
        "current_directory": os.getcwd()
    }

def fetch_data():
    db_path = get_db_path()
    if not db_path:
        raise HTTPException(status_code=500, detail="Database file (kasuwa.db) not found on server.")
    
    try:
        conn = sqlite3.connect(db_path)
        # Ensure your SQLite table is named 'prices'
        df = pd.read_sql("SELECT * FROM prices", conn)
        conn.close()
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database read error: {str(e)}")

@app.get("/intelligence/{commodity}")
def get_intelligence(commodity: str):
    info = COMMODITY_INFO.get(commodity)
    if not info:
        return {"info": {"desc": "Market data pending for this commodity.", "markets": "N/A"}}
    return {"info": info}

@app.get("/analysis")
def full_analysis(commodity: str, month: str, years: str = "", market: str = "All Markets"):
    df = fetch_data()
    
    # 1. Map actual DB columns to code names
    # Using 'kg per bag' directly from your DB schema
    df = df.rename(columns={
        'timestamp': 'Start Time',
        'price of bag': 'Price per Bag',
        'kg per bag': 'kg per bag', 
        'commodity': 'Commodity',
        'market': 'Market'
    })

    # 2. Ensure numbers are numeric (handles strings or commas in data)
    df['Price per Bag'] = pd.to_numeric(df['Price per Bag'], errors='coerce')
    df['kg per bag'] = pd.to_numeric(df['kg per bag'], errors='coerce')
    
    # 3. Create the price_per_kg column for the charts
    # We calculate this on the fly: Price / Weight
    df['price_per_kg'] = df['Price per Bag'] / df['kg per bag']

    # 4. Date conversion
    df['Start Time'] = pd.to_datetime(df['Start Time'])
    df['month_name'] = df['Start Time'].dt.strftime('%B')
    
    # 5. Filtering (Case-insensitive for robustness)
    df = df[df['Commodity'].str.lower() == commodity.lower()]
    df = df[df['month_name'].str.lower() == month.lower()]
    
    if market != "All Markets":
        df = df[df['Market'].str.lower() == market.lower()]

    # 6. Safety Check
    if df.empty:
        return {"chart_data": [], "metrics": {"avg": 0, "max": 0, "min": 0}}

    # 7. Final Output
    return {
        "chart_data": df[['Market', 'price_per_kg', 'Price per Bag', 'Start Time']].to_dict(orient='records'),
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
    
    db_path = get_db_path()
    if not db_path:
        raise HTTPException(status_code=500, detail="Database not found")

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO prices ("Start Time", "Market", "Commodity", "Price per Bag", "price_per_kg")
            VALUES (?, ?, ?, ?, ?)
        """, (data['date'], data['market'], data['commodity'], data['price_bag'], data['price_kg']))
        conn.commit()
        conn.close()
        return {"status": "success", "message": f"Added record for {data['commodity']}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")