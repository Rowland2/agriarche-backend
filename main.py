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
    "http://localhost:5173",
    "http://localhost:8080",
    "http://localhost:4200",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:3000",
]

origins_prod = [
    "https://agriarche-pricing-pfadctddnsfn2mqob8snwe.streamlit.app",
    "https://app.kasuwa.com",
    "https://app.kasuwa.com/dashboard/sourcing-insights",
    "https://devext.kasuwa.com/dashboard/sourcing-insights",
    "https://devext.kasuwa.com",
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

# --- ENHANCED CROP INTELLIGENCE DATA ---
CROP_INTELLIGENCE = {
    "soya beans": {
        "desc": "A raw leguminous crop used for oil and feed.",
        "markets": "Mubi, Giwa, and Kumo",
        "abundance": "Nov, Dec, and April",
        "note": "A key industrial driver for the poultry and vegetable oil sectors."
    },
    "soybeans": {
        "desc": "A raw leguminous crop used for oil and feed.",
        "markets": "Mubi, Giwa, and Kumo",
        "abundance": "Nov, Dec, and April",
        "note": "A key industrial driver for the poultry and vegetable oil sectors."
    },
    "brown cowpea": {
        "desc": "Protein-rich legume popular in local diets.",
        "markets": "Dawanau and Potiskum",
        "abundance": "Oct through Jan",
        "note": "Supply depends on Northern storage."
    },
    "cowpea brown": {
        "desc": "Protein-rich legume popular in local diets.",
        "markets": "Dawanau and Potiskum",
        "abundance": "Oct through Jan",
        "note": "Supply depends on Northern storage."
    },
    "white cowpea": {
        "desc": "Staple bean variety used for commercial flour.",
        "markets": "Dawanau and Bodija",
        "abundance": "Oct and Nov",
        "note": "High demand in South drives prices."
    },
    "cowpea white": {
        "desc": "Staple bean variety used for commercial flour.",
        "markets": "Dawanau and Bodija",
        "abundance": "Oct and Nov",
        "note": "High demand in South drives prices."
    },
    "honey beans": {
        "desc": "Premium sweet brown beans (Oloyin).",
        "markets": "Oyingbo and Dawanau",
        "abundance": "Oct to Dec",
        "note": "Often carries a price premium."
    },
    "white maize": {
        "desc": "Primary cereal crop for food and industry.",
        "markets": "Giwa, Makarfi, and Funtua",
        "abundance": "Sept to Nov",
        "note": "Correlates strongly with Sorghum trends."
    },
    "maize white": {
        "desc": "Primary cereal crop for food and industry.",
        "markets": "Giwa, Makarfi, and Funtua",
        "abundance": "Sept to Nov",
        "note": "Correlates strongly with Sorghum trends."
    },
    "rice paddy": {
        "desc": "Raw rice before milling/processing.",
        "markets": "Argungu and Kano",
        "abundance": "Nov and Dec",
        "note": "Foundation for processed rice pricing."
    },
    "paddy rice": {
        "desc": "Raw rice before milling/processing.",
        "markets": "Argungu and Kano",
        "abundance": "Nov and Dec",
        "note": "Foundation for processed rice pricing."
    },
    "processed rice": {
        "desc": "Milled and polished local rice.",
        "markets": "Kano, Lagos, and Onitsha",
        "abundance": "Year-round",
        "note": "Price fluctuates with fuel/milling costs."
    },
    "rice processed": {
        "desc": "Milled and polished local rice.",
        "markets": "Kano, Lagos, and Onitsha",
        "abundance": "Year-round",
        "note": "Price fluctuates with fuel/milling costs."
    },
    "red sorghum": {
        "desc": "Drought-resistant grain staple.",
        "markets": "Dawanau and Gombe",
        "abundance": "Dec and Jan",
        "note": "Market substitute for Maize."
    },
    "sorghum red": {
        "desc": "Drought-resistant grain staple.",
        "markets": "Dawanau and Gombe",
        "abundance": "Dec and Jan",
        "note": "Market substitute for Maize."
    },
    "white sorghum": {
        "desc": "Drought-resistant white grain variety.",
        "markets": "Dawanau and Gombe",
        "abundance": "Dec and Jan",
        "note": "Premium variety for food processing."
    },
    "sorghum white": {
        "desc": "Drought-resistant white grain variety.",
        "markets": "Dawanau and Gombe",
        "abundance": "Dec and Jan",
        "note": "Premium variety for food processing."
    },
    "yellow sorghum": {
        "desc": "Yellow grain sorghum variety.",
        "markets": "Dawanau and Gombe",
        "abundance": "Dec and Jan",
        "note": "Used for brewing and animal feed."
    },
    "sorghum yellow": {
        "desc": "Yellow grain sorghum variety.",
        "markets": "Dawanau and Gombe",
        "abundance": "Dec and Jan",
        "note": "Used for brewing and animal feed."
    },
    "sorghum": {
        "desc": "General sorghum variety.",
        "markets": "Dawanau and Gombe",
        "abundance": "Dec and Jan",
        "note": "Versatile grain for food and industry."
    },
    "millet": {
        "desc": "Fast-growing cereal for the lean season.",
        "markets": "Dawanau and Potiskum",
        "abundance": "Sept and Oct",
        "note": "First harvest after rainy season."
    },
    "groundnut gargaja": {
        "desc": "Local peanut variety for oil extraction.",
        "markets": "Dawanau and Gombe",
        "abundance": "Oct and Nov",
        "note": "Sahel region specialty."
    },
    "groundnut kampala": {
        "desc": "Large, premium roasting groundnuts.",
        "markets": "Kano and Dawanau",
        "abundance": "Oct and Nov",
        "note": "Higher oil content than Gargaja."
    },
    "groundnuts (peanuts)": {
        "desc": "General groundnut/peanut variety.",
        "markets": "Northern markets",
        "abundance": "Oct and Nov",
        "note": "Key oilseed crop with diverse uses."
    }
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


@app.get("/prices-with-change")
def get_prices_with_change(
    page: Optional[int] = 1,
    page_size: Optional[int] = 100,
    commodity: Optional[str] = None
):
    """
    Get prices with percentage change calculation

    Examples:
    - /prices-with-change?page=1&page_size=20
    - /prices-with-change?commodity=Brown Cowpea&page=1&page_size=20
    """
    try:
        df = fetch_data()

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

        # Convert dates
        df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')

        # Filter by commodity if provided
        if commodity:
            df = df[df['commodity'].str.contains(commodity, case=False, na=False)]

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

        # Sort by commodity and date (newest first)
        df = df.sort_values(['commodity', 'start_time'], ascending=[True, False])

        # Convert price to numeric
        df['price_per_kg_numeric'] = pd.to_numeric(df['price_per_kg'], errors='coerce')

        # Calculate % change for each commodity
        # This compares each row to the previous row within the same commodity
        df['percent_change'] = df.groupby('commodity')['price_per_kg_numeric'].pct_change(periods=-1) * 100
        df['percent_change'] = df['percent_change'].round(2)

        # Add change indicator emoji
        df['change_indicator'] = df['percent_change'].apply(
            lambda x: '📈' if pd.notna(x) and x > 0
            else '📉' if pd.notna(x) and x < 0
            else '➡️'
        )

        # Replace NaN with "N/A" string
        df['percent_change'] = df['percent_change'].apply(
            lambda x: str(x) if pd.notna(x) else 'N/A'
        )

        # Convert dates back to string for JSON
        df['start_time'] = df['start_time'].astype(str)

        # Pagination
        total_records = len(df)
        total_pages = (total_records + page_size - 1) // page_size if total_records > 0 else 0

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
        import traceback
        print(f"Error in prices-with-change: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch prices: {str(e)}")


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
                },
                "filters_applied": {
                    "commodity": commodity,
                    "location": location
                }
            }

        if 'date' in df.columns:
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

        if 'date' in df.columns:
            df['date'] = df['date'].astype(str)

        required_cols = ['date', 'commodity', 'location', 'unit', 'price']
        for col in required_cols:
            if col not in df.columns:
                df[col] = ''
        df = df[required_cols]

        total_records = len(df)
        total_pages = (total_records + page_size - 1) // page_size if total_records > 0 else 0

        if page < 1:
            page = 1
        if total_pages > 0 and page > total_pages:
            page = total_pages

        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        df_page = df.iloc[start_idx:end_idx] if total_records > 0 else df

        return {
            "data": df_page.to_dict(orient='records'),
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_records,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            },
            "filters_applied": {
                "commodity": commodity,
                "location": location,
                "min_price": min_price,
                "max_price": max_price,
                "start_date": start_date,
                "end_date": end_date
            }
        }

    except Exception as e:
        import traceback
        error_detail = f"Filtered other sources failed: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)


@app.get("/intelligence/{commodity}")
def get_intelligence(commodity: str):
    """
    Get detailed market intelligence for a commodity.
    Returns description, key markets, abundance periods, and notes.
    """
    commodity_lower = commodity.lower().strip()

    info = CROP_INTELLIGENCE.get(
        commodity_lower,
        {
            "desc": "Market intelligence currently being updated for this commodity.",
            "markets": "Multiple Northern hubs",
            "abundance": "Seasonal",
            "note": "Price trends are being monitored."
        }
    )

    return {
        "commodity": commodity,
        "intelligence": info
    }


@app.get("/ai-market-advisor/{commodity}")
def get_ai_market_advisor(commodity: str, month: Optional[str] = None):
    """
    Get AI-powered market recommendations and insights

    Examples:
    - /ai-market-advisor/Brown Cowpea
    - /ai-market-advisor/Brown Cowpea?month=January
    """
    try:
        # Get all price data
        df = fetch_data()

        if df.empty:
            return {
                "commodity": commodity,
                "advice": "No price data available yet. Please check back after data is collected.",
                "confidence": "low",
                "recommendations": []
            }

        df['start_time'] = pd.to_datetime(df['start_time'])

        # Filter by commodity
        df_commodity = df[df['commodity'].str.contains(commodity, case=False, na=False)]

        if df_commodity.empty:
            available = sorted(df['commodity'].unique().tolist())
            return {
                "commodity": commodity,
                "advice": f"Commodity '{commodity}' not found in database. Please check spelling or try another commodity.",
                "confidence": "low",
                "recommendations": [],
                "available_commodities": available[:10]
            }

        # Filter by month if provided
        if month:
            df_commodity['month_name'] = df_commodity['start_time'].dt.strftime('%B')
            df_commodity = df_commodity[df_commodity['month_name'].str.lower() == month.lower()]

            if df_commodity.empty:
                return {
                    "commodity": commodity,
                    "advice": f"No price data available for {commodity} in {month}. Try a different month.",
                    "confidence": "low",
                    "recommendations": []
                }

        # Calculate price statistics
        df_commodity['price_per_kg'] = pd.to_numeric(df_commodity['price_per_kg'], errors='coerce')
        df_commodity = df_commodity.dropna(subset=['price_per_kg'])

        if df_commodity.empty or len(df_commodity) < 2:
            return {
                "commodity": commodity,
                "advice": "Insufficient price data for analysis. At least 2 price records needed.",
                "confidence": "low",
                "recommendations": []
            }

        avg_price = df_commodity['price_per_kg'].mean()
        std_dev = df_commodity['price_per_kg'].std()

        # Find best and worst markets
        market_avg = df_commodity.groupby('market')['price_per_kg'].mean()

        if len(market_avg) == 0:
            return {
                "commodity": commodity,
                "advice": "Price data exists but no market information available.",
                "confidence": "low",
                "recommendations": []
            }

        best_market = market_avg.idxmin()
        worst_market = market_avg.idxmax()
        best_price = market_avg.min()
        worst_price = market_avg.max()

        # Calculate price trend
        trend = "stable"
        trend_percent = 0

        if len(df_commodity) >= 14:
            try:
                sorted_data = df_commodity.sort_values('start_time')
                recent_avg = sorted_data.tail(7)['price_per_kg'].mean()
                previous_avg = sorted_data.iloc[-14:-7]['price_per_kg'].mean()

                if pd.notna(recent_avg) and pd.notna(previous_avg) and previous_avg > 0:
                    diff = recent_avg - previous_avg
                    if abs(diff) > previous_avg * 0.02:  # More than 2% change
                        trend = "rising" if diff > 0 else "falling"
                        trend_percent = abs((diff / previous_avg) * 100)
            except:
                trend = "stable"
                trend_percent = 0

        # Generate AI advice
        if trend == "rising":
            price_advice = f"Prices are trending upward ({trend_percent:.1f}% increase). Consider sourcing soon before further increases."
        elif trend == "falling":
            price_advice = f"Prices are trending downward ({trend_percent:.1f}% decrease). You may benefit from waiting if possible."
        else:
            price_advice = "Prices are stable. Good time to source at predictable rates."

        # Market recommendation
        if best_market != worst_market:
            savings = worst_price - best_price
            savings_percent = (savings / worst_price * 100) if worst_price > 0 else 0
            market_advice = f" Buy from {best_market} (₦{best_price:.2f}/kg) and save ₦{savings:.2f}/kg ({savings_percent:.1f}%) compared to {worst_market}."
        else:
            market_advice = f" Average price across markets: ₦{avg_price:.2f}/kg."

        # Volatility assessment
        if pd.notna(std_dev) and avg_price > 0:
            volatility = "high" if std_dev > avg_price * 0.2 else "moderate" if std_dev > avg_price * 0.1 else "low"
        else:
            volatility = "low"

        volatility_advice = f" Market volatility is {volatility}."

        full_advice = price_advice + market_advice + volatility_advice

        return {
            "commodity": commodity,
            "advice": full_advice,
            "confidence": "high" if len(df_commodity) >= 10 else "moderate" if len(df_commodity) >= 5 else "low",
            "trend": trend,
            "trend_percentage": round(trend_percent, 2),
            "recommendations": [
                {
                    "type": "best_market",
                    "market": best_market,
                    "price_per_kg": round(float(best_price), 2),
                    "reason": "Lowest average price"
                },
                {
                    "type": "avoid_market",
                    "market": worst_market,
                    "price_per_kg": round(float(worst_price), 2),
                    "reason": "Highest average price"
                }
            ],
            "market_insights": {
                "average_price": round(float(avg_price), 2),
                "price_range": {
                    "min": round(float(market_avg.min()), 2),
                    "max": round(float(market_avg.max()), 2)
                },
                "volatility": volatility,
                "data_points": len(df_commodity)
            }
        }

    except Exception as e:
        import traceback
        error_detail = f"AI advisor failed: {str(e)}\n{traceback.format_exc()}"
        print(error_detail)  # Log to console for debugging

        return {
            "commodity": commodity,
            "advice": "Unable to generate market advice at this time. Please try again later.",
            "confidence": "low",
            "recommendations": [],
            "error": str(e)
        }


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
        return {
            "chart_data": [],
            "metrics": {
                "price_per_kg": {"avg": 0, "max": 0, "min": 0},
                "price_per_bag": {"avg": 0, "max": 0, "min": 0}
            },
            "strategic_sourcing": None
        }

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

        market_avg = market_avg[
            (market_avg['price_per_kg'] > 0) &
            (market_avg['price_per_bag'] > 0)
        ]

        if not market_avg.empty:
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

    chart_data_df = df[['market', 'price_per_kg', 'price_per_bag', 'start_time', 'commodity']].copy()
    chart_data_df['start_time'] = chart_data_df['start_time'].astype(str)
    chart_data_df['price_per_kg'] = chart_data_df['price_per_kg'].astype(str)
    chart_data_df['price_per_bag'] = chart_data_df['price_per_bag'].astype(str)

    return {
        "chart_data": chart_data_df.to_dict(orient='records'),
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
    """
    Get filtered prices with automatic % change calculation

    ✅ NOW INCLUDES: percent_change and change_indicator fields automatically!
    """
    try:
        df = fetch_data()

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

        # Convert dates
        if 'start_time' in df.columns:
            df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')

        # Apply all filters
        if commodity:
            df = df[df['commodity'].str.contains(commodity, case=False, na=False)]
        if market:
            df = df[df['market'].str.contains(market, case=False, na=False)]
        if state:
            df = df[df['state'].str.contains(state, case=False, na=False)]

        # Convert price to numeric for filtering and calculations
        df['price_per_kg_numeric'] = pd.to_numeric(df['price_per_kg'], errors='coerce')

        if min_price is not None:
            df = df[df['price_per_kg_numeric'] >= min_price]
        if max_price is not None:
            df = df[df['price_per_kg_numeric'] <= max_price]
        if start_date:
            df = df[df['start_time'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['start_time'] <= pd.to_datetime(end_date)]

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

        # Sort by commodity and date (newest first) for % change calculation
        df = df.sort_values(['commodity', 'start_time'], ascending=[True, False])

        # ✅ CALCULATE % CHANGE
        # Compare each row to the previous row within the same commodity
        df['percent_change'] = df.groupby('commodity')['price_per_kg_numeric'].pct_change(periods=-1) * 100
        df['percent_change'] = df['percent_change'].round(2)

        # Add change indicator emoji
        df['change_indicator'] = df['percent_change'].apply(
            lambda x: '📈' if pd.notna(x) and x > 0
            else '📉' if pd.notna(x) and x < 0
            else '➡️'
        )

        # Format percent_change for display
        df['percent_change'] = df['percent_change'].apply(
            lambda x: f"+{x}%" if pd.notna(x) and x > 0
            else f"{x}%" if pd.notna(x) and x < 0
            else "None"
        )

        # Convert dates back to string for JSON
        df['start_time'] = df['start_time'].astype(str)

        # Pagination
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
        import traceback
        print(f"Error in prices/filtered: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Filtered query failed: {str(e)}")


# ============================================================
# FILTER ENDPOINTS (OPTIMIZED)
# ============================================================

@app.get("/filters/commodities")
def get_commodity_list():
    try:
        query = text("""
            SELECT DISTINCT TRIM(commodity) AS commodity
            FROM prices
            WHERE commodity IS NOT NULL AND commodity != ''
            ORDER BY commodity
        """)
        with engine.connect() as conn:
            result = conn.execute(query)
            commodities = [row[0] for row in result]

        return {"commodities": commodities, "count": len(commodities)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch commodities: {str(e)}")


@app.get("/filters/markets")
def get_market_list():
    try:
        query = text("""
            SELECT DISTINCT TRIM(market) AS market
            FROM prices
            WHERE market IS NOT NULL AND market != ''
            ORDER BY market
        """)
        with engine.connect() as conn:
            result = conn.execute(query)
            markets = [row[0] for row in result]

        return {"markets": markets, "count": len(markets)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch markets: {str(e)}")


@app.get("/filters/states")
def get_state_list():
    try:
        query = text("""
            SELECT DISTINCT TRIM(state) AS state
            FROM prices
            WHERE state IS NOT NULL AND state != ''
            ORDER BY state
        """)
        with engine.connect() as conn:
            result = conn.execute(query)
            states = [row[0] for row in result]

        return {"states": states, "count": len(states)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch states: {str(e)}")


@app.get("/filters/years")
def get_year_list():
    try:
        query = text("""
            SELECT DISTINCT EXTRACT(YEAR FROM start_time) AS year
            FROM prices
            WHERE start_time IS NOT NULL
            ORDER BY year DESC
        """)
        with engine.connect() as conn:
            result = conn.execute(query)
            years = [str(int(row[0])) for row in result]

        return {"years": years, "count": len(years)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch years: {str(e)}")


@app.get("/filters/other-sources-locations")
def get_other_sources_locations():
    try:
        query = text("""
            SELECT DISTINCT TRIM(location) AS location
            FROM other_sources
            WHERE location IS NOT NULL AND location != ''
            ORDER BY location
        """)
        with engine.connect() as conn:
            result = conn.execute(query)
            locations = [row[0] for row in result]

        return {"locations": locations, "count": len(locations)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch locations: {str(e)}")


@app.get("/filters/other-sources-commodities")
def get_other_sources_commodities():
    try:
        query = text("""
            SELECT DISTINCT TRIM(commodity) AS commodity
            FROM other_sources
            WHERE commodity IS NOT NULL AND commodity != ''
            ORDER BY commodity
        """)
        with engine.connect() as conn:
            result = conn.execute(query)
            commodities = [row[0] for row in result]

        return {"commodities": commodities, "count": len(commodities)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch other sources commodities: {str(e)}")


@app.get("/filters/all")
def get_all_filters():
    """
    Get all filter values in ONE database call
    Much faster for dashboards.
    """
    try:

        commodities_query = """
            SELECT DISTINCT TRIM(commodity) AS commodity
            FROM prices
            WHERE commodity IS NOT NULL AND commodity != ''
            ORDER BY commodity
        """

        markets_query = """
            SELECT DISTINCT TRIM(market) AS market
            FROM prices
            WHERE market IS NOT NULL AND market != ''
            ORDER BY market
        """

        states_query = """
            SELECT DISTINCT TRIM(state) AS state
            FROM prices
            WHERE state IS NOT NULL AND state != ''
            ORDER BY state
        """

        years_query = """
            SELECT DISTINCT EXTRACT(YEAR FROM start_time) AS year
            FROM prices
            WHERE start_time IS NOT NULL
            ORDER BY year DESC
        """

        other_commodities_query = """
            SELECT DISTINCT TRIM(commodity) AS commodity
            FROM other_sources
            WHERE commodity IS NOT NULL AND commodity != ''
            ORDER BY commodity
        """

        other_locations_query = """
            SELECT DISTINCT TRIM(location) AS location
            FROM other_sources
            WHERE location IS NOT NULL AND location != ''
            ORDER BY location
        """

        with engine.connect() as conn:

            commodities = [row[0] for row in conn.execute(text(commodities_query))]
            markets = [row[0] for row in conn.execute(text(markets_query))]
            states = [row[0] for row in conn.execute(text(states_query))]
            years = [str(int(row[0])) for row in conn.execute(text(years_query))]

            other_commodities = [row[0] for row in conn.execute(text(other_commodities_query))]
            other_locations = [row[0] for row in conn.execute(text(other_locations_query))]

        return {
            "commodities": commodities,
            "markets": markets,
            "states": states,
            "years": years,
            "months": [
                "January","February","March","April","May","June",
                "July","August","September","October","November","December"
            ],
            "other_sources": {
                "commodities": other_commodities,
                "locations": other_locations
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch filters: {str(e)}")


# ============================================================
# MARKET COMPARISON ENDPOINT
# ============================================================

@app.get("/market-comparison")
def get_market_comparison(commodity: str, month: str):
    """
    Get market comparison - ONLY returns markets that carry the selected commodity
    
    ✅ NEW: Filters out markets that don't have the commodity
    """
    try:
        # 1. Get internal prices (Kasuwa data)
        df_internal = fetch_data()
        df_internal['start_time'] = pd.to_datetime(df_internal['start_time'])
        df_internal['month_name'] = df_internal['start_time'].dt.strftime('%B')
        
        # Filter by commodity AND month
        df_internal = df_internal[
            (df_internal['commodity'].str.contains(commodity, case=False, na=False)) &
            (df_internal['month_name'].str.lower() == month.lower())
        ]
        df_internal['price_per_kg'] = pd.to_numeric(df_internal['price_per_kg'], errors='coerce')
        
        # Remove zero/null prices
        df_internal = df_internal[df_internal['price_per_kg'] > 0]

        internal_markets = []
        if not df_internal.empty:
            for mkt in df_internal['market'].unique():
                mkt_data = df_internal[df_internal['market'] == mkt]
                avg_price = mkt_data['price_per_kg'].mean()
                
                # ✅ Only add if price is valid
                if avg_price > 0:
                    internal_markets.append({
                        "source": "Internal (Kasuwa)",
                        "market": mkt,
                        "avg_price_per_kg": round(float(avg_price), 2),
                        "min_price": float(mkt_data['price_per_kg'].min()),
                        "max_price": float(mkt_data['price_per_kg'].max())
                    })

        # 2. Get external sources
        df_external = fetch_other_sources_data()
        df_external['date'] = pd.to_datetime(df_external['date'], errors='coerce')
        df_external['month_name'] = df_external['date'].dt.strftime('%B')
        
        # Filter by commodity AND month
        df_external = df_external[
            (df_external['commodity'].str.contains(commodity, case=False, na=False)) &
            (df_external['month_name'].str.lower() == month.lower())
        ]
        df_external['price'] = pd.to_numeric(df_external['price'], errors='coerce')
        
        # Remove zero/null prices
        df_external = df_external[df_external['price'] > 0]

        external_markets = []
        if not df_external.empty:
            for loc in df_external['location'].unique():
                loc_data = df_external[df_external['location'] == loc]
                avg_bag = loc_data['price'].mean()
                
                # ✅ Only add if price is valid
                if avg_bag > 0:
                    is_bag = loc_data['unit'].iloc[0] == 'bag'
                    avg_kg = avg_bag / 100 if is_bag else avg_bag
                    
                    external_markets.append({
                        "source": "External",
                        "market": loc,
                        "avg_price_per_kg": round(float(avg_kg), 2),
                        "min_price": float(loc_data['price'].min() / 100 if is_bag else loc_data['price'].min()),
                        "max_price": float(loc_data['price'].max() / 100 if is_bag else loc_data['price'].max())
                    })

        # 3. Combine and sort by price
        all_markets = internal_markets + external_markets
        all_markets.sort(key=lambda x: x['avg_price_per_kg'])

        # ✅ If no markets found, return helpful message
        if not all_markets:
            return {
                "commodity": commodity,
                "month": month,
                "markets": [],
                "total_markets": 0,
                "internal_count": 0,
                "external_count": 0,
                "message": f"No markets found carrying '{commodity}' in {month}. Try a different month or commodity."
            }

        return {
            "commodity": commodity,
            "month": month,
            "markets": all_markets,
            "total_markets": len(all_markets),
            "internal_count": len(internal_markets),
            "external_count": len(external_markets)
        }

    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500, 
            detail=f"Market comparison failed: {str(e)}\n{traceback.format_exc()}"
        )

# ============================================================
# TWO-MARKET COMPARISON ENDPOINT
# ============================================================

@app.get("/compare-two-markets")
def compare_two_markets(
    commodity: str,
    month: str,
    market1: str,
    market2: str,
    source1: str = "internal",
    source2: str = "internal"
):
    """
    Compare prices between two specific markets.

    Parameters:
    - commodity: Commodity name
    - month: Month name
    - market1: First market name
    - market2: Second market name
    - source1: "internal" or "external" (default: "internal")
    - source2: "internal" or "external" (default: "internal")

    Examples:
    - /compare-two-markets?commodity=Cowpea Brown&month=January&market1=Lashe Money&market2=Potiskum
    - /compare-two-markets?commodity=Soybeans&month=January&market1=Potiskum&market2=Dawanau Market, Kano State&source1=internal&source2=external
    - /compare-two-markets?commodity=Soybeans&month=January&market1=Dawanau Market, Kano State&market2=Achau Market, Kaduna State&source1=external&source2=external
    """
    try:
        def get_market_data(commodity, month, market, source):
            if source.lower() == "internal":
                df = fetch_data()
                df['start_time'] = pd.to_datetime(df['start_time'])
                df['month_name'] = df['start_time'].dt.strftime('%B')

                commodity_df = df[df['commodity'].str.contains(commodity, case=False, na=False)]
                if commodity_df.empty:
                    return {
                        "found": False,
                        "error": f"Commodity '{commodity}' not found in internal data",
                        "available_commodities": sorted(df['commodity'].unique().tolist())
                    }

                market_commodity_df = commodity_df[
                    commodity_df['market'].str.lower() == market.lower()
                ]
                if market_commodity_df.empty:
                    return {
                        "found": False,
                        "error": f"'{market}' does not carry '{commodity}' in internal data",
                        "markets_that_carry_this_commodity": sorted(commodity_df['market'].unique().tolist())
                    }

                month_df = market_commodity_df[
                    market_commodity_df['month_name'].str.lower() == month.lower()
                ]
                if month_df.empty:
                    return {
                        "found": False,
                        "error": f"'{market}' has no '{commodity}' data for {month}",
                        "available_months_for_this_market": sorted(market_commodity_df['month_name'].unique().tolist())
                    }

                month_df['price_per_kg'] = pd.to_numeric(month_df['price_per_kg'], errors='coerce')
                month_df['price_per_bag'] = pd.to_numeric(month_df['price_per_bag'], errors='coerce')

                return {
                    "found": True,
                    "market": market,
                    "source": "Internal (Kasuwa)",
                    "avg_price_per_kg": round(float(month_df['price_per_kg'].mean()), 2),
                    "avg_price_per_bag": round(float(month_df['price_per_bag'].mean()), 2),
                    "min_price_per_kg": float(month_df['price_per_kg'].min()),
                    "max_price_per_kg": float(month_df['price_per_kg'].max()),
                    "record_count": len(month_df)
                }

            else:  # external
                df = fetch_other_sources_data()
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df['month_name'] = df['date'].dt.strftime('%B')

                commodity_df = df[df['commodity'].str.contains(commodity, case=False, na=False)]
                if commodity_df.empty:
                    return {
                        "found": False,
                        "error": f"Commodity '{commodity}' not found in external data",
                        "available_commodities": sorted(df['commodity'].unique().tolist())
                    }

                market_commodity_df = commodity_df[
                    commodity_df['location'].str.contains(market, case=False, na=False)
                ]
                if market_commodity_df.empty:
                    return {
                        "found": False,
                        "error": f"'{market}' does not carry '{commodity}' in external data",
                        "locations_that_carry_this_commodity": sorted(commodity_df['location'].unique().tolist())
                    }

                month_df = market_commodity_df[
                    market_commodity_df['month_name'].str.lower() == month.lower()
                ]
                if month_df.empty:
                    return {
                        "found": False,
                        "error": f"'{market}' has no '{commodity}' data for {month}",
                        "available_months_for_this_location": sorted(market_commodity_df['month_name'].unique().tolist())
                    }

                month_df['price'] = pd.to_numeric(month_df['price'], errors='coerce')
                is_bag = month_df['unit'].iloc[0] == 'bag'
                avg_price = month_df['price'].mean()
                avg_kg = avg_price / 100 if is_bag else avg_price

                return {
                    "found": True,
                    "market": market,
                    "source": "External",
                    "avg_price_per_kg": round(float(avg_kg), 2),
                    "avg_price_per_bag": round(float(avg_price if is_bag else avg_kg * 100), 2),
                    "min_price_per_kg": float(month_df['price'].min() / 100 if is_bag else month_df['price'].min()),
                    "max_price_per_kg": float(month_df['price'].max() / 100 if is_bag else month_df['price'].max()),
                    "record_count": len(month_df)
                }

        market1_data = get_market_data(commodity, month, market1, source1)
        market2_data = get_market_data(commodity, month, market2, source2)

        if not market1_data.get("found") or not market2_data.get("found"):
            return {
                "success": False,
                "commodity": commodity,
                "month": month,
                "market1_status": market1_data if not market1_data.get("found") else {"found": True, "market": market1},
                "market2_status": market2_data if not market2_data.get("found") else {"found": True, "market": market2},
                "tip": "Check 'available_months_for_this_market' or 'markets_that_carry_this_commodity' above for valid values."
            }

        price_diff_kg = market2_data['avg_price_per_kg'] - market1_data['avg_price_per_kg']
        price_diff_bag = market2_data['avg_price_per_bag'] - market1_data['avg_price_per_bag']
        percentage_diff = (
            price_diff_kg / market1_data['avg_price_per_kg'] * 100
        ) if market1_data['avg_price_per_kg'] > 0 else 0

        if market1_data['avg_price_per_kg'] < market2_data['avg_price_per_kg']:
            cheaper_market = market1
            more_expensive = market2
        else:
            cheaper_market = market2
            more_expensive = market1

        return {
            "success": True,
            "commodity": commodity,
            "month": month,
            "market1": market1_data,
            "market2": market2_data,
            "comparison": {
                "cheaper_market": cheaper_market,
                "more_expensive_market": more_expensive,
                "price_difference_per_kg": round(abs(price_diff_kg), 2),
                "price_difference_per_bag": round(abs(price_diff_bag), 2),
                "percentage_difference": round(abs(percentage_diff), 2),
                "savings_per_kg": round(abs(price_diff_kg), 2),
                "savings_per_bag": round(abs(price_diff_bag), 2)
            }
        }

    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"Comparison failed: {str(e)}\n{traceback.format_exc()}"
        )


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