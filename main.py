from fastapi import FastAPI, Security, HTTPException, status, Depends
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy import create_engine, text
import pandas as pd
import os

app = FastAPI(title="Agriarche Data Hub")

# --- DATABASE SETUP (NEON CLOUD) ---
DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)

# --- SECURITY ---
API_KEY = "Agriarche_Internal_Key_2026"
api_key_header = APIKeyHeader(name="access_token")

# --- CROP INTELLIGENCE DATA ---
CROP_INTELLIGENCE = {
    "maize white": "Maize is a staple energy source. Current trends show price stability due to recent harvests.",
    "soya beans": "High demand for poultry feed continues to drive soy prices globally and locally.",
    "rice paddy": "Local rice production is increasing; keep an eye on milling costs and fuel prices for transport.",
    "cowpea white": "A major protein source. Prices typically fluctuate based on storage availability in the North.",
    "groundnut gargaja": "A key oilseed. Market volume is steady, with strong interest from local processors.",
    "millet": "Resilient to dry weather, millet remains a vital food security crop in arid regions.",
    "sorghum red": "Primarily used for industrial brewing and local flour; demand remains consistent."
}

@app.get("/")
def home():
    return {
        "status": "Agriarche API is online", 
        "database": "Connected to Neon Cloud",
        "current_directory": os.getcwd()
    }

def fetch_data():
    """Fetch Kasuwa internal prices data"""
    try:
        return pd.read_sql("SELECT * FROM prices", engine)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cloud Database Error: {str(e)}")

def fetch_other_sources_data():
    """Fetch other sources (scraped) data"""
    try:
        return pd.read_sql("SELECT * FROM other_sources ORDER BY date DESC", engine)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Other Sources Database Error: {str(e)}")

@app.get("/prices")
def get_all_prices():
    """Returns all data from the prices table (Kasuwa internal data)."""
    try:
        df = fetch_data()
        if 'start_time' in df.columns:
            df['start_time'] = df['start_time'].astype(str)
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Verification failed: {str(e)}")

@app.get("/other-sources")
def get_other_sources():
    """Returns all data from the other_sources table (scraped data) - EXACT MATCH FOR SCREENSHOT"""
    try:
        df = fetch_other_sources_data()
        
        # Convert date to string for JSON serialization
        if 'date' in df.columns:
            df['date'] = df['date'].astype(str)
        
        # Ensure all required columns are present
        required_cols = ['date', 'commodity', 'location', 'unit', 'price']
        for col in required_cols:
            if col not in df.columns:
                df[col] = ''
        
        return df[required_cols].to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Other sources fetch failed: {str(e)}")

@app.get("/intelligence/{commodity}")
def get_intelligence(commodity: str):
    """Provides market descriptions for the Streamlit info box."""
    desc = CROP_INTELLIGENCE.get(commodity.lower(), "Market intelligence currently being updated for this commodity.")
    return {"info": {"desc": desc}}

@app.get("/analysis")
def full_analysis(commodity: str, month: str, market: str = "All Markets"):
    """Analysis endpoint for Kasuwa internal prices"""
    df = fetch_data() 
    
    # 1. Date Processing
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['month_name'] = df['start_time'].dt.strftime('%B')
    
    # 2. Filtering
    df = df[df['commodity'].str.lower() == commodity.lower()]
    df = df[df['month_name'].str.lower() == month.lower()]
    
    if market != "All Markets":
        df = df[df['market'].str.lower() == market.lower()]

    if df.empty:
        return {"chart_data": [], "metrics": {"avg": 0, "max": 0, "min": 0}}

    # 3. Numeric Safety
    df['price_per_kg'] = pd.to_numeric(df['price_per_kg'], errors='coerce').fillna(0)
    df['price_per_bag'] = pd.to_numeric(df['price_per_bag'], errors='coerce').fillna(0)

    return {
        "chart_data": df[['market', 'price_per_kg', 'price_per_bag', 'start_time']].astype(str).to_dict(orient='records'),
        "metrics": {
            "avg": round(float(df['price_per_kg'].mean()), 2),
            "max": float(df['price_per_kg'].max()),
            "min": float(df['price_per_kg'].min())
        }
    }

@app.post("/update-price")
def update_price(data: dict, token: str = Depends(api_key_header)):
    """Add new record to prices table (Kasuwa internal)"""
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")

    try:
        with engine.begin() as conn:
            query = text("""
                INSERT INTO prices (start_time, agent_code, state, market, commodity, price_per_bag, weight_of_bag_kg, price_per_kg, availability, commodity_type)
                VALUES (:start_time, :agent_code, :state, :market, :commodity, :price_per_bag, :weight_of_bag_kg, :price_per_kg, :availability, :commodity_type)
            """)
            conn.execute(query, data)
        return {"status": "success", "message": f"Added record for {data['commodity']}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")

@app.post("/bulk-upload-other-sources")
def bulk_upload_other_sources(records: list, token: str = Depends(api_key_header)):
    """
    Bulk upload multiple records to other_sources table
    Expected format for each record:
    {
        "date": "2026-01-29 15:30:00",
        "commodity": "Maize",
        "location": "Giwa Market, Kaduna State",
        "unit": "bag",
        "price": 25000
    }
    """
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")

    try:
        with engine.begin() as conn:
            query = text("""
                INSERT INTO other_sources (date, commodity, location, unit, price)
                VALUES (:date, :commodity, :location, :unit, :price)
            """)
            for record in records:
                conn.execute(query, record)
        return {"status": "success", "message": f"Added {len(records)} other sources records"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bulk upload failed: {str(e)}")