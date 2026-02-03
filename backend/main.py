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
    import os
    db_exists = os.path.exists('kasuwa.db') or os.path.exists('../kasuwa.db')
    return {
        "status": "Agriarche API is online", 
        "database_file_found": db_exists,
        "current_directory": os.getcwd()
    }

@app.get("/intelligence/{commodity}")
def get_intelligence(commodity: str):
    info = COMMODITY_INFO.get(commodity)
    if not info:
        return {"info": {"desc": "Market data pending for this commodity.", "markets": "N/A"}}
    return {"info": info}

def fetch_data():
    # LOOK FOR THE DATABASE, NOT THE EXCEL FILE
    db_path = 'kasuwa.db' if os.path.exists('kasuwa.db') else '../kasuwa.db'
    try:
        conn = sqlite3.connect(db_path)
        # Make sure the table name is 'prices'
        df = pd.read_sql("SELECT * FROM prices", conn)
        conn.close()
        return df
    except Exception as e:
        # This will tell us if the database itself is missing
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analysis")
def full_analysis(commodity: str, month: str, years: str = "", market: str = "All Markets"):
    df = fetch_data()
    
    # DEBUG PRINTS: Check Render Logs for these!
    print(f"--- FETCHED {len(df)} ROWS FROM DATABASE ---")
    print(f"--- LOOKING FOR: {commodity} in {month} ---")

    # 1. Date Conversion
    df['Start Time'] = pd.to_datetime(df['Start Time'])
    df['month_name'] = df['Start Time'].dt.strftime('%B')
    
    # 2. Filtering (Matching Excel Capitalization)
    df = df[df['Commodity'] == commodity]
    print(f"Rows after commodity filter: {len(df)}")
    
    df = df[df['month_name'] == month]
    print(f"Rows after month filter: {len(df)}")
    
    if market != "All Markets":
        df = df[df['Market'] == market]
        print(f"Rows after market filter: {len(df)}")

    # 3. SAFETY CHECK: Return early if empty to avoid math crashes
    if df.empty:
        print("!!! DATASET IS EMPTY - RETURNING ZEROES !!!")
        return {
            "chart_data": [], 
            "metrics": {"avg": 0, "max": 0, "min": 0}
        }

    # 4. Success: Prepare data for the orange Plotly chart
    # Use 'Start Time' for the X-axis as seen in your screenshot
    result = df[['Market', 'price_per_kg', 'Price per Bag', 'Start Time']].to_dict(orient='records')
    
    return {
        "chart_data": result,
        "metrics": {
            "avg": round(float(df['price_per_kg'].mean()), 2),
            "max": float(df['price_per_kg'].max()),
            "min": float(df['price_per_kg'].min())
        }
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
