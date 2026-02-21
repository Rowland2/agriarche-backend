from fastapi import FastAPI, Security, HTTPException, status, Depends, Body
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import pandas as pd
import os
from typing import List, Optional
from pydantic import BaseModel

app = FastAPI(title="Agriarche Data Hub")

# ============================================================
# CORS CONFIGURATION
# ============================================================

# Development origins (localhost for testing)
origins_dev = [
    "http://localhost:3000",
    "http://localhost:5173",  # Vite (tech team's dev server)
    "http://localhost:8080",  # Vue
    "http://localhost:4200",  # Angular
    "http://127.0.0.1:5173",
    "http://127.0.0.1:3000",
]

# Production origins (your actual websites)
origins_prod = [
    "https://agriarche-pricing-pfadctddnsfn2mqob8snwe.streamlit.app",  # Your Streamlit dashboard
    "https://www.gofarmrate.com",  # Your company website
    "https://gofarmrate.com",  # Your company website (without www)
]

# Combine all allowed origins
allowed_origins = origins_dev + origins_prod

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- PYDANTIC MODELS ---
class OtherSourceRecord(BaseModel):
    date: str
    commodity: str
    location: str
    unit: str
    price: float

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
        "current_directory": os.getcwd(),
        "cors_enabled": True
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
def get_all_prices(
    page: Optional[int] = 1,
    page_size: Optional[int] = 100
):
    """
    Returns all data from the prices table (Kasuwa internal data) with pagination
    
    Parameters:
    - page: Page number (default: 1)
    - page_size: Records per page (default: 100, max: 1000)
    
    Returns paginated price data with metadata
    """
    try:
        df = fetch_data()
        
        # Convert datetime to string
        if 'start_time' in df.columns:
            df['start_time'] = df['start_time'].astype(str)
        
        # Calculate pagination
        total_records = len(df)
        total_pages = (total_records + page_size - 1) // page_size
        
        # Validate page number
        if page < 1:
            page = 1
        if page > total_pages and total_pages > 0:
            page = total_pages
        
        # Apply pagination
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        df_page = df.iloc[start_idx:end_idx]
        
        return {
            "data": df_page.to_dict(orient='records'),
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Verification failed: {str(e)}")

@app.get("/other-sources")
def get_other_sources(
    page: Optional[int] = 1,
    page_size: Optional[int] = 100
):
    """
    Returns all data from the other_sources table (scraped data) with pagination
    
    Parameters:
    - page: Page number (default: 1)
    - page_size: Records per page (default: 100, max: 1000)
    """
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
        
        df = df[required_cols]
        
        # Calculate pagination
        total_records = len(df)
        total_pages = (total_records + page_size - 1) // page_size
        
        # Validate page number
        if page < 1:
            page = 1
        if page > total_pages and total_pages > 0:
            page = total_pages
        
        # Apply pagination
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        df_page = df.iloc[start_idx:end_idx]
        
        return {
            "data": df_page.to_dict(orient='records'),
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Other sources fetch failed: {str(e)}")

@app.get("/intelligence/{commodity}")
def get_intelligence(commodity: str):
    """Provides market descriptions for the Streamlit info box."""
    desc = CROP_INTELLIGENCE.get(commodity.lower(), "Market intelligence currently being updated for this commodity.")
    return {"info": {"desc": desc}}

@app.get("/analysis")
def full_analysis(commodity: str, month: str, market: str = "All Markets", exact: bool = False):
    """
    Analysis endpoint for Kasuwa internal prices
    
    Parameters:
    - commodity: Commodity to search for
    - month: Month name (e.g., "January")
    - market: Market name or "All Markets"
    - exact: If True, use exact match. If False, use partial match (default)
    
    Examples:
    - /analysis?commodity=Maize&exact=true       → Only "Maize" (exact)
    - /analysis?commodity=Maize&exact=false      → All Maize varieties (partial)
    - /analysis?commodity=Maize White&exact=true → Only "Maize White" (exact)
    """
    # Debug: Log what we received
    print(f"DEBUG: Received commodity='{commodity}', month='{month}', market='{market}', exact={exact}")
    
    df = fetch_data()
    
    print(f"DEBUG: Total records before filtering: {len(df)}")
    
    # 1. Date Processing
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['month_name'] = df['start_time'].dt.strftime('%B')
    
    # 2. Filtering by commodity
    if commodity:
        if exact:
            # Exact match - "Maize" only matches "Maize", not "Maize White"
            df = df[df['commodity'].str.lower() == commodity.lower()]
            print(f"DEBUG: Using EXACT match for '{commodity}': {len(df)} records")
        else:
            # Partial match - "Maize" matches "Maize", "Maize White", etc.
            df = df[df['commodity'].str.contains(commodity, case=False, na=False)]
            print(f"DEBUG: Using PARTIAL match for '{commodity}': {len(df)} records")
    
    # Then filter by month
    df = df[df['month_name'].str.lower() == month.lower()]
    print(f"DEBUG: After month filter ({month}): {len(df)} records")
    
    # Then filter by market if specified
    if market != "All Markets":
        df = df[df['market'].str.lower() == market.lower()]
        print(f"DEBUG: After market filter ({market}): {len(df)} records")

    if df.empty:
        print("DEBUG: No data found after filtering!")
        return {"chart_data": [], "metrics": {"avg": 0, "max": 0, "min": 0}}

    # 3. Numeric Safety
    df['price_per_kg'] = pd.to_numeric(df['price_per_kg'], errors='coerce').fillna(0)
    df['price_per_bag'] = pd.to_numeric(df['price_per_bag'], errors='coerce').fillna(0)
    
    # Debug: Show unique commodities in result
    unique_commodities = df['commodity'].unique().tolist()
    print(f"DEBUG: Unique commodities in result: {unique_commodities}")
    
    # 4. Calculate Strategic Sourcing (Best Buy & Worst Market)
    strategic_sourcing = None
    if not df.empty and len(df['market'].unique()) > 1:
        # Group by market and calculate average prices
        market_avg = df.groupby('market').agg({
            'price_per_kg': 'mean',
            'price_per_bag': 'mean'
        }).reset_index()
        
        # Find cheapest and most expensive markets
        cheapest_idx = market_avg['price_per_kg'].idxmin()
        expensive_idx = market_avg['price_per_kg'].idxmax()
        
        strategic_sourcing = {
            "best_buy": {
                "market": market_avg.loc[cheapest_idx, 'market'],
                "price_per_kg": round(float(market_avg.loc[cheapest_idx, 'price_per_kg']), 2),
                "price_per_bag": round(float(market_avg.loc[cheapest_idx, 'price_per_bag']), 2)
            },
            "worst_market": {
                "market": market_avg.loc[expensive_idx, 'market'],
                "price_per_kg": round(float(market_avg.loc[expensive_idx, 'price_per_kg']), 2),
                "price_per_bag": round(float(market_avg.loc[expensive_idx, 'price_per_bag']), 2)
            }
        }

    return {
        "chart_data": df[['market', 'price_per_kg', 'price_per_bag', 'start_time', 'commodity']].astype(str).to_dict(orient='records'),
        "metrics": {
            "avg": round(float(df['price_per_kg'].mean()), 2),
            "max": float(df['price_per_kg'].max()),
            "min": float(df['price_per_kg'].min())
        },
        "strategic_sourcing": strategic_sourcing
    }

# ============================================================
# NEW: FILTERED PRICES ENDPOINT WITH PAGINATION
# ============================================================

@app.get("/prices/filtered")
def get_filtered_prices(
    commodity: Optional[str] = None,
    market: Optional[str] = None,
    state: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page: int = 1,
    page_size: int = 100
):
    """
    Get filtered and paginated prices
    
    Query Parameters:
    - commodity: Filter by commodity name
    - market: Filter by market name
    - state: Filter by state
    - min_price: Minimum price per kg
    - max_price: Maximum price per kg
    - start_date: Start date (YYYY-MM-DD)
    - end_date: End date (YYYY-MM-DD)
    - page: Page number (default: 1)
    - page_size: Records per page (default: 100, max: 1000)
    """
    
    try:
        # Start with base query
        query = "SELECT * FROM prices WHERE 1=1"
        params = {}
        
        # Add filters dynamically
        if commodity:
            query += " AND LOWER(commodity) LIKE LOWER(:commodity)"
            params['commodity'] = f"%{commodity}%"
        
        if market:
            query += " AND LOWER(market) LIKE LOWER(:market)"
            params['market'] = f"%{market}%"
        
        if state:
            query += " AND LOWER(state) LIKE LOWER(:state)"
            params['state'] = f"%{state}%"
        
        if min_price is not None:
            query += " AND price_per_kg >= :min_price"
            params['min_price'] = min_price
        
        if max_price is not None:
            query += " AND price_per_kg <= :max_price"
            params['max_price'] = max_price
        
        if start_date:
            query += " AND start_time >= :start_date"
            params['start_date'] = start_date
        
        if end_date:
            query += " AND start_time <= :end_date"
            params['end_date'] = end_date
        
        # Add ordering
        query += " ORDER BY start_time DESC"
        
        # Execute query
        df = pd.read_sql(text(query), engine, params=params)
        
        # Calculate pagination
        total_records = len(df)
        total_pages = (total_records + page_size - 1) // page_size
        
        # Validate page number
        if page < 1:
            page = 1
        if page > total_pages and total_pages > 0:
            page = total_pages
        
        # Apply pagination
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        df_page = df.iloc[start_idx:end_idx]
        
        # Convert to JSON
        if 'start_time' in df_page.columns:
            df_page['start_time'] = df_page['start_time'].astype(str)
        
        return {
            "data": df_page.to_dict(orient='records'),
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Filtered query failed: {str(e)}")

# ============================================================
# NEW: DYNAMIC FILTER OPTIONS ENDPOINTS
# ============================================================

@app.get("/filters/commodities")
def get_commodity_list():
    """Get list of unique commodities from database"""
    try:
        query = "SELECT DISTINCT commodity FROM prices WHERE commodity IS NOT NULL ORDER BY commodity"
        df = pd.read_sql(text(query), engine)
        
        commodities = df['commodity'].tolist()
        
        return {
            "commodities": commodities,
            "count": len(commodities)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch commodities: {str(e)}")


@app.get("/filters/markets")
def get_market_list():
    """Get list of unique markets from database"""
    try:
        query = "SELECT DISTINCT market FROM prices WHERE market IS NOT NULL ORDER BY market"
        df = pd.read_sql(text(query), engine)
        
        markets = df['market'].tolist()
        
        return {
            "markets": markets,
            "count": len(markets)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch markets: {str(e)}")


@app.get("/filters/states")
def get_state_list():
    """Get list of unique states from database"""
    try:
        query = "SELECT DISTINCT state FROM prices WHERE state IS NOT NULL ORDER BY state"
        df = pd.read_sql(text(query), engine)
        
        states = df['state'].tolist()
        
        return {
            "states": states,
            "count": len(states)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch states: {str(e)}")


@app.get("/filters/years")
def get_year_list():
    """Get list of years with data in database"""
    try:
        query = "SELECT DISTINCT EXTRACT(YEAR FROM start_time) as year FROM prices ORDER BY year DESC"
        df = pd.read_sql(text(query), engine)
        
        years = [str(int(year)) for year in df['year'].tolist()]
        
        return {
            "years": years,
            "count": len(years)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch years: {str(e)}")


@app.get("/filters/all")
def get_all_filters():
    """Get all filter options in one call (more efficient)"""
    try:
        # Get commodities
        query_commodities = "SELECT DISTINCT commodity FROM prices WHERE commodity IS NOT NULL ORDER BY commodity"
        commodities_df = pd.read_sql(text(query_commodities), engine)
        
        # Get markets
        query_markets = "SELECT DISTINCT market FROM prices WHERE market IS NOT NULL ORDER BY market"
        markets_df = pd.read_sql(text(query_markets), engine)
        
        # Get states
        query_states = "SELECT DISTINCT state FROM prices WHERE state IS NOT NULL ORDER BY state"
        states_df = pd.read_sql(text(query_states), engine)
        
        # Get years
        query_years = "SELECT DISTINCT EXTRACT(YEAR FROM start_time) as year FROM prices ORDER BY year DESC"
        years_df = pd.read_sql(text(query_years), engine)
        
        return {
            "commodities": commodities_df['commodity'].tolist(),
            "markets": markets_df['market'].tolist(),
            "states": states_df['state'].tolist(),
            "years": [str(int(year)) for year in years_df['year'].tolist()],
            "months": ["January", "February", "March", "April", "May", "June", 
                      "July", "August", "September", "October", "November", "December"]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch filters: {str(e)}")

# ============================================================
# EXISTING ENDPOINTS (UNCHANGED)
# ============================================================

@app.get("/gap-analysis")
def get_gap_analysis(
    month: str,
    page: int = 1,
    page_size: int = 20
):
    """
    Generate gap analysis showing min, max, avg prices and best/worst markets
    for all commodities in a given month (with pagination)
    
    Parameters:
    - month: Month name (e.g., "January", "February")
    - page: Page number (default: 1)
    - page_size: Records per page (default: 20, max: 100)
    
    Returns: Paginated list of commodities with price analysis
    
    Example:
    /gap-analysis?month=January&page=1&page_size=10
    """
    try:
        # Fetch all data
        df = fetch_data()
        
        # Process dates
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['month_name'] = df['start_time'].dt.strftime('%B')
        
        # Filter by month
        df = df[df['month_name'].str.lower() == month.lower()]
        
        if df.empty:
            return {
                "data": [],
                "pagination": {
                    "page": 1,
                    "page_size": page_size,
                    "total_records": 0,
                    "total_pages": 0,
                    "has_next": False,
                    "has_previous": False
                }
            }
        
        # Ensure numeric prices
        df['price_per_kg'] = pd.to_numeric(df['price_per_kg'], errors='coerce')
        df = df.dropna(subset=['price_per_kg'])
        
        # Group by commodity and calculate stats
        results = []
        
        for commodity in df['commodity'].unique():
            commodity_df = df[df['commodity'] == commodity]
            
            # Calculate stats
            min_price = commodity_df['price_per_kg'].min()
            max_price = commodity_df['price_per_kg'].max()
            avg_price = commodity_df['price_per_kg'].mean()
            
            # Find cheapest and most expensive markets
            market_avg = commodity_df.groupby('market')['price_per_kg'].mean()
            cheapest_market = market_avg.idxmin()
            top_selling_market = market_avg.idxmax()
            
            results.append({
                "commodity": commodity,
                "min_price": round(float(min_price), 2),
                "max_price": round(float(max_price), 2),
                "avg_price": round(float(avg_price), 2),
                "cheapest_source": cheapest_market,
                "top_selling_market": top_selling_market
            })
        
        # Sort by commodity name
        results = sorted(results, key=lambda x: x['commodity'])
        
        # Calculate pagination
        total_records = len(results)
        total_pages = (total_records + page_size - 1) // page_size
        
        # Validate page number
        if page < 1:
            page = 1
        if page > total_pages and total_pages > 0:
            page = total_pages
        
        # Apply pagination
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_results = results[start_idx:end_idx]
        
        return {
            "data": paginated_results,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gap analysis failed: {str(e)}")


# ============================================================
# EXISTING ENDPOINTS (UNCHANGED)
# ============================================================

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
def bulk_upload_other_sources(records: List[OtherSourceRecord], token: str = Depends(api_key_header)):
    """Bulk upload multiple records to other_sources table"""
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")

    try:
        with engine.begin() as conn:
            query = text("""
                INSERT INTO other_sources (date, commodity, location, unit, price)
                VALUES (:date, :commodity, :location, :unit, :price)
            """)
            for record in records:
                conn.execute(query, record.dict())
        return {"status": "success", "message": f"Added {len(records)} other sources records"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bulk upload failed: {str(e)}")
