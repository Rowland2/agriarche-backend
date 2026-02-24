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

origins_dev = [
    "http://localhost:3000",
    "http://localhost:5173",  # Vite (tech team's dev server)
    "http://localhost:8080",  # Vue
    "http://localhost:4200",  # Angular
    "http://127.0.0.1:5173",
    "http://127.0.0.1:3000",
]

origins_prod = [
    "https://agriarche-pricing-pfadctddnsfn2mqob8snwe.streamlit.app",
    "https://www.gofarmrate.com",
    "https://gofarmrate.com",
]

allowed_origins = origins_dev + origins_prod

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
    try:
        df = fetch_data()
        if 'start_time' in df.columns:
            df['start_time'] = df['start_time'].astype(str)
        total_records = len(df)
        total_pages = (total_records + page_size - 1) // page_size
        if page < 1:
            page = 1
        if page > total_pages and total_pages > 0:
            page = total_pages
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
    try:
        df = fetch_other_sources_data()
        if 'date' in df.columns:
            df['date'] = df['date'].astype(str)
        required_cols = ['date', 'commodity', 'location', 'unit', 'price']
        for col in required_cols:
            if col not in df.columns:
                df[col] = ''
        df = df[required_cols]
        total_records = len(df)
        total_pages = (total_records + page_size - 1) // page_size
        if page < 1:
            page = 1
        if page > total_pages and total_pages > 0:
            page = total_pages
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


@app.get("/other-sources/filtered")
def get_other_sources_filtered(
    commodity: Optional[str] = None,
    location: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page: Optional[int] = 1,
    page_size: Optional[int] = 100
):
    try:
        df = fetch_other_sources_data()
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        if commodity:
            df = df[df['commodity'].str.contains(commodity, case=False, na=False)]
        if location:
            df = df[df['location'].str.contains(location, case=False, na=False)]
        if min_price is not None:
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df = df[df['price'] >= min_price]
        if max_price is not None:
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df = df[df['price'] <= max_price]
        if start_date:
            df = df[df['date'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['date'] <= pd.to_datetime(end_date)]
        df['date'] = df['date'].astype(str)
        required_cols = ['date', 'commodity', 'location', 'unit', 'price']
        for col in required_cols:
            if col not in df.columns:
                df[col] = ''
        df = df[required_cols]
        total_records = len(df)
        total_pages = (total_records + page_size - 1) // page_size
        if page < 1:
            page = 1
        if page > total_pages and total_pages > 0:
            page = total_pages
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
        raise HTTPException(status_code=500, detail=f"Filtered other sources failed: {str(e)}")


@app.get("/intelligence/{commodity}")
def get_intelligence(commodity: str):
    """Provides market descriptions for the Streamlit info box."""
    desc = CROP_INTELLIGENCE.get(commodity.lower(), "Market intelligence currently being updated for this commodity.")
    return {"info": {"desc": desc}}


@app.get("/analysis")
def full_analysis(commodity: str, month: str, market: str = "All Markets", exact: bool = False):
    print(f"DEBUG: Received commodity='{commodity}', month='{month}', market='{market}', exact={exact}")
    df = fetch_data()
    print(f"DEBUG: Total records before filtering: {len(df)}")
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['month_name'] = df['start_time'].dt.strftime('%B')
    if commodity:
        if exact:
            df = df[df['commodity'].str.lower() == commodity.lower()]
            print(f"DEBUG: Using EXACT match for '{commodity}': {len(df)} records")
        else:
            df = df[df['commodity'].str.contains(commodity, case=False, na=False)]
            print(f"DEBUG: Using PARTIAL match for '{commodity}': {len(df)} records")
    df = df[df['month_name'].str.lower() == month.lower()]
    print(f"DEBUG: After month filter ({month}): {len(df)} records")
    if market != "All Markets":
        df = df[df['market'].str.lower() == market.lower()]
        print(f"DEBUG: After market filter ({market}): {len(df)} records")
    if df.empty:
        print("DEBUG: No data found after filtering!")
        return {"chart_data": [], "metrics": {"avg": 0, "max": 0, "min": 0}}
    df['price_per_kg'] = pd.to_numeric(df['price_per_kg'], errors='coerce').fillna(0)
    df['price_per_bag'] = pd.to_numeric(df['price_per_bag'], errors='coerce').fillna(0)
    unique_commodities = df['commodity'].unique().tolist()
    print(f"DEBUG: Unique commodities in result: {unique_commodities}")
    strategic_sourcing = None
    if not df.empty and len(df['market'].unique()) > 1:
        market_avg = df.groupby('market').agg({
            'price_per_kg': 'mean',
            'price_per_bag': 'mean'
        }).reset_index()
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
            "price_per_kg": {
                "avg": round(float(df['price_per_kg'].mean()), 2),
                "max": float(df['price_per_kg'].max()),
                "min": float(df['price_per_kg'].min())
            },
            "price_per_bag": {
                "avg": round(float(df['price_per_bag'].mean()), 2),
                "max": float(df['price_per_bag'].max()),
                "min": float(df['price_per_bag'].min())
            }
        },
        "strategic_sourcing": strategic_sourcing
    }


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
    try:
        df = fetch_data()
        if 'start_time' in df.columns:
            df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
        if commodity:
            df = df[df['commodity'].str.contains(commodity, case=False, na=False)]
        if market:
            df = df[df['market'].str.contains(market, case=False, na=False)]
        if state:
            df = df[df['state'].str.contains(state, case=False, na=False)]
        if min_price is not None:
            df['price_per_kg'] = pd.to_numeric(df['price_per_kg'], errors='coerce')
            df = df[df['price_per_kg'] >= min_price]
        if max_price is not None:
            df['price_per_kg'] = pd.to_numeric(df['price_per_kg'], errors='coerce')
            df = df[df['price_per_kg'] <= max_price]
        if start_date:
            df = df[df['start_time'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['start_time'] <= pd.to_datetime(end_date)]
        df['start_time'] = df['start_time'].astype(str)
        df = df.sort_values('start_time', ascending=False)
        total_records = len(df)
        total_pages = (total_records + page_size - 1) // page_size
        if page < 1:
            page = 1
        if page > total_pages and total_pages > 0:
            page = total_pages
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        df_page = df.iloc[start_idx:end_idx]
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
# FILTER ENDPOINTS
# ============================================================

@app.get("/filters/commodities")
def get_commodity_list():
    try:
        query = "SELECT DISTINCT commodity FROM prices WHERE commodity IS NOT NULL ORDER BY commodity"
        df = pd.read_sql(text(query), engine)
        return {"commodities": df['commodity'].tolist(), "count": len(df)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch commodities: {str(e)}")


@app.get("/filters/markets")
def get_market_list():
    try:
        query = "SELECT DISTINCT market FROM prices WHERE market IS NOT NULL ORDER BY market"
        df = pd.read_sql(text(query), engine)
        return {"markets": df['market'].tolist(), "count": len(df)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch markets: {str(e)}")


@app.get("/filters/states")
def get_state_list():
    try:
        query = "SELECT DISTINCT state FROM prices WHERE state IS NOT NULL ORDER BY state"
        df = pd.read_sql(text(query), engine)
        return {"states": df['state'].tolist(), "count": len(df)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch states: {str(e)}")


@app.get("/filters/years")
def get_year_list():
    try:
        query = "SELECT DISTINCT EXTRACT(YEAR FROM start_time) as year FROM prices ORDER BY year DESC"
        df = pd.read_sql(text(query), engine)
        return {"years": [str(int(y)) for y in df['year'].tolist()], "count": len(df)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch years: {str(e)}")


@app.get("/filters/other-sources-locations")
def get_other_sources_locations():
    try:
        query = "SELECT DISTINCT location FROM other_sources WHERE location IS NOT NULL ORDER BY location"
        df = pd.read_sql(text(query), engine)
        return {"locations": df['location'].tolist(), "count": len(df)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch locations: {str(e)}")


@app.get("/filters/other-sources-commodities")
def get_other_sources_commodities():
    try:
        query = "SELECT DISTINCT commodity FROM other_sources WHERE commodity IS NOT NULL ORDER BY commodity"
        df = pd.read_sql(text(query), engine)
        return {"commodities": df['commodity'].tolist(), "count": len(df)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch other sources commodities: {str(e)}")


@app.get("/filters/all")
def get_all_filters():
    """Get all filter options in one call (more efficient)"""
    try:
        commodities_df = pd.read_sql(
            text("SELECT DISTINCT commodity FROM prices WHERE commodity IS NOT NULL ORDER BY commodity"), engine)
        markets_df = pd.read_sql(
            text("SELECT DISTINCT market FROM prices WHERE market IS NOT NULL ORDER BY market"), engine)
        states_df = pd.read_sql(
            text("SELECT DISTINCT state FROM prices WHERE state IS NOT NULL ORDER BY state"), engine)
        years_df = pd.read_sql(
            text("SELECT DISTINCT EXTRACT(YEAR FROM start_time) as year FROM prices ORDER BY year DESC"), engine)
        other_commodities_df = pd.read_sql(
            text("SELECT DISTINCT commodity FROM other_sources WHERE commodity IS NOT NULL ORDER BY commodity"), engine)
        other_locations_df = pd.read_sql(
            text("SELECT DISTINCT location FROM other_sources WHERE location IS NOT NULL ORDER BY location"), engine)

        return {
            "commodities": commodities_df['commodity'].tolist(),
            "markets": markets_df['market'].tolist(),
            "states": states_df['state'].tolist(),
            "years": [str(int(y)) for y in years_df['year'].tolist()],
            "months": ["January", "February", "March", "April", "May", "June",
                       "July", "August", "September", "October", "November", "December"],
            "other_sources": {
                "commodities": other_commodities_df['commodity'].tolist(),
                "locations": other_locations_df['location'].tolist()
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch filters: {str(e)}")


# ============================================================
# MARKET COMPARISON ENDPOINT
# ============================================================

@app.get("/market-comparison")
def get_market_comparison(commodity: str, month: str):
    """Get market comparison including BOTH internal and external sources"""
    try:
        # 1. Internal prices (Kasuwa data)
        df_internal = fetch_data()
        df_internal['start_time'] = pd.to_datetime(df_internal['start_time'])
        df_internal['month_name'] = df_internal['start_time'].dt.strftime('%B')
        df_internal = df_internal[
            (df_internal['commodity'].str.contains(commodity, case=False, na=False)) &
            (df_internal['month_name'].str.lower() == month.lower())
        ]
        df_internal['price_per_kg'] = pd.to_numeric(df_internal['price_per_kg'], errors='coerce')

        internal_markets = []
        if not df_internal.empty:
            for mkt in df_internal['market'].unique():
                mkt_data = df_internal[df_internal['market'] == mkt]
                internal_markets.append({
                    "source": "Internal (Kasuwa)",
                    "market": mkt,
                    "avg_price_per_kg": round(float(mkt_data['price_per_kg'].mean()), 2),
                    "min_price": float(mkt_data['price_per_kg'].min()),
                    "max_price": float(mkt_data['price_per_kg'].max())
                })

        # 2. External sources
        df_external = fetch_other_sources_data()
        df_external['date'] = pd.to_datetime(df_external['date'], errors='coerce')
        df_external['month_name'] = df_external['date'].dt.strftime('%B')
        df_external = df_external[
            (df_external['commodity'].str.contains(commodity, case=False, na=False)) &
            (df_external['month_name'].str.lower() == month.lower())
        ]
        df_external['price'] = pd.to_numeric(df_external['price'], errors='coerce')

        external_markets = []
        if not df_external.empty:
            for loc in df_external['location'].unique():
                loc_data = df_external[df_external['location'] == loc]
                avg_bag = loc_data['price'].mean()
                is_bag = loc_data['unit'].iloc[0] == 'bag'
                avg_kg = avg_bag / 100 if is_bag else avg_bag
                external_markets.append({
                    "source": "External",
                    "market": loc,
                    "avg_price_per_kg": round(float(avg_kg), 2),
                    "min_price": float(loc_data['price'].min() / 100 if is_bag else loc_data['price'].min()),
                    "max_price": float(loc_data['price'].max() / 100 if is_bag else loc_data['price'].max())
                })

        # 3. Combine and sort
        all_markets = internal_markets + external_markets
        all_markets.sort(key=lambda x: x['avg_price_per_kg'])

        return {
            "commodity": commodity,
            "month": month,
            "markets": all_markets,
            "total_markets": len(all_markets),
            "internal_count": len(internal_markets),
            "external_count": len(external_markets)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Market comparison failed: {str(e)}")


# ============================================================
# GAP ANALYSIS ENDPOINT
# ============================================================

@app.get("/gap-analysis")
def get_gap_analysis(
    month: str,
    page: int = 1,
    page_size: int = 20
):
    try:
        df = fetch_data()
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['month_name'] = df['start_time'].dt.strftime('%B')
        df = df[df['month_name'].str.lower() == month.lower()]

        if df.empty:
            return {
                "data": [],
                "pagination": {
                    "page": 1, "page_size": page_size,
                    "total_records": 0, "total_pages": 0,
                    "has_next": False, "has_previous": False
                }
            }

        df['price_per_kg'] = pd.to_numeric(df['price_per_kg'], errors='coerce')
        df = df.dropna(subset=['price_per_kg'])

        results = []
        for commodity in df['commodity'].unique():
            commodity_df = df[df['commodity'] == commodity]
            market_avg = commodity_df.groupby('market')['price_per_kg'].mean()
            results.append({
                "commodity": commodity,
                "min_price": round(float(commodity_df['price_per_kg'].min()), 2),
                "max_price": round(float(commodity_df['price_per_kg'].max()), 2),
                "avg_price": round(float(commodity_df['price_per_kg'].mean()), 2),
                "cheapest_source": market_avg.idxmin(),
                "top_selling_market": market_avg.idxmax()
            })

        results = sorted(results, key=lambda x: x['commodity'])
        total_records = len(results)
        total_pages = (total_records + page_size - 1) // page_size
        if page < 1:
            page = 1
        if page > total_pages and total_pages > 0:
            page = total_pages
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size

        return {
            "data": results[start_idx:end_idx],
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
# WRITE / UPLOAD ENDPOINTS
# ============================================================

@app.post("/update-price")
def update_price(data: dict, token: str = Depends(api_key_header)):
    """Add new record to prices table (Kasuwa internal)"""
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")
    try:
        with engine.begin() as conn:
            query = text("""
                INSERT INTO prices (start_time, agent_code, state, market, commodity,
                    price_per_bag, weight_of_bag_kg, price_per_kg, availability, commodity_type)
                VALUES (:start_time, :agent_code, :state, :market, :commodity,
                    :price_per_bag, :weight_of_bag_kg, :price_per_kg, :availability, :commodity_type)
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