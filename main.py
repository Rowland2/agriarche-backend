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

@app.get("/")
def home():
    return {
        "status": "Agriarche API is online", 
        "database": "Connected to Neon Cloud",
        "current_directory": os.getcwd()
    }

def fetch_data():
    try:
        # Pulls data directly from the table 'prices'
        return pd.read_sql("SELECT * FROM prices", engine)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cloud Database Error: {str(e)}")

# --- NEW VERIFICATION ROUTE ---
@app.get("/prices")
def get_all_prices():
    """Returns all data from the database to verify the connection is working."""
    try:
        df = fetch_data()
        # Convert timestamp objects to strings for JSON compatibility
        if 'start_time' in df.columns:
            df['start_time'] = df['start_time'].astype(str)
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Verification failed: {str(e)}")

@app.get("/analysis")
def full_analysis(commodity: str, month: str, market: str = "All Markets"):
    df = fetch_data() 
    
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['month_name'] = df['start_time'].dt.strftime('%B')
    
    df = df[df['commodity'].str.lower() == commodity.lower()]
    df = df[df['month_name'].str.lower() == month.lower()]
    
    if market != "All Markets":
        df = df[df['market'].str.lower() == market.lower()]

    if df.empty:
        return {"chart_data": [], "metrics": {"avg": 0, "max": 0, "min": 0}}

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