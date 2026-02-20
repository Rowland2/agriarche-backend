"""
SMART Upload Script for Other Sources (Scraped Data)
This version only uploads NEW records and avoids duplicates!
"""

import pandas as pd
import requests
from datetime import datetime

# =====================================================
# CONFIGURATION
# =====================================================
API_URL = "https://agriarche-backend.onrender.com"
API_KEY = "Agriarche_Internal_Key_2026"
HEADERS = {"access_token": API_KEY}

# =====================================================
# SMART UPLOAD - ONLY NEW RECORDS
# =====================================================
def upload_new_other_sources(excel_file_path):
    """
    Smart upload that only uploads NEW other sources records
    Checks database first to avoid duplicates
    """
    
    print("=" * 70)
    print("SMART UPLOAD - OTHER SOURCES (NEW RECORDS ONLY)")
    print("=" * 70)
    
    # Read File (Auto-detect CSV or Excel)
    print(f"\n📂 Reading: {excel_file_path}")
    try:
        if excel_file_path.lower().endswith('.csv'):
            df = pd.read_csv(excel_file_path)
            print(f"✅ Loaded {len(df)} rows from CSV")
        elif excel_file_path.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(excel_file_path)
            print(f"✅ Loaded {len(df)} rows from Excel")
        else:
            # Try CSV first, then Excel
            try:
                df = pd.read_csv(excel_file_path)
                print(f"✅ Loaded {len(df)} rows from CSV")
            except:
                df = pd.read_excel(excel_file_path)
                print(f"✅ Loaded {len(df)} rows from Excel")
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return
    
    print(f"📊 Columns found: {df.columns.tolist()}")
    
    # Map columns
    print("\n🔄 Mapping columns...")
    col_map = {}
    
    for col in df.columns:
        col_lower = col.lower().strip()
        
        if any(x in col_lower for x in ['date', 'time', 'timestamp', 'scraped']):
            col_map[col] = 'date'
        elif 'commodity' in col_lower:
            col_map[col] = 'commodity'
        elif any(x in col_lower for x in ['location', 'market', 'place']):
            col_map[col] = 'location'
        elif 'unit' in col_lower:
            col_map[col] = 'unit'
        elif any(x in col_lower for x in ['price', 'amount', 'cost']):
            col_map[col] = 'price'
        elif 'source' in col_lower:
            col_map[col] = 'source'
    
    df = df.rename(columns=col_map)
    
    for old, new in col_map.items():
        print(f"   {old} → {new}")
    
    # Verify required columns
    required = ['date', 'commodity', 'location', 'unit', 'price']
    missing = [c for c in required if c not in df.columns]
    
    if missing:
        print(f"\n❌ Missing required columns: {missing}")
        print(f"Available: {df.columns.tolist()}")
        print("\n💡 Required columns:")
        print("   - date (date/time)")
        print("   - commodity (commodity name)")
        print("   - location (market/location name)")
        print("   - unit (e.g., 'bag', 'kg')")
        print("   - price (price value)")
        return
    
    print("✅ All required columns found")
    
    # Clean data
    print("\n🧹 Cleaning data...")
    
    # Date
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    
    # Clean text columns FIRST
    df['commodity'] = df['commodity'].astype(str).str.strip()
    df['location'] = df['location'].astype(str).str.strip()
    df['unit'] = df['unit'].astype(str).str.strip()
    
    # Add source if not present
    if 'source' not in df.columns:
        df['source'] = 'web_scraping'
    else:
        df['source'] = df['source'].astype(str).str.strip()
    
    # Price
    df['price'] = df['price'].astype(str).str.replace(',', '').str.replace('₦', '').str.strip()
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df = df.dropna(subset=['price'])
    
    print(f"✅ {len(df)} valid rows in file")
    
    # Fetch existing data from database
    print("\n🔍 Checking database for existing records...")
    try:
        # Fetch all existing records (use large page size to get everything)
        all_existing_data = []
        page = 1
        
        while True:
            response = requests.get(
                f"{API_URL}/other-sources",
                params={"page": page, "page_size": 1000},
                headers=HEADERS
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # Handle paginated response
                if isinstance(result, dict) and 'data' in result:
                    data = result['data']
                    pagination = result.get('pagination', {})
                    
                    all_existing_data.extend(data)
                    
                    # Check if there are more pages
                    if not pagination.get('has_next', False):
                        break
                    
                    page += 1
                elif isinstance(result, list):
                    # Old format (non-paginated)
                    all_existing_data = result
                    break
                else:
                    break
            else:
                print(f"⚠️  API returned status {response.status_code}")
                break
        
        if all_existing_data:
            existing_data = pd.DataFrame(all_existing_data)
            
            if not existing_data.empty:
                existing_data['date'] = pd.to_datetime(existing_data['date'], errors='coerce')
                existing_data['commodity'] = existing_data['commodity'].astype(str).str.strip().str.lower()
                existing_data['location'] = existing_data['location'].astype(str).str.strip().str.lower()
                
                print(f"📊 Database has {len(existing_data)} existing records")
                
                # Create unique identifier for deduplication
                # Using: date + commodity + location (without time for broader matching)
                df['unique_key'] = (
                    df['date'].dt.strftime('%Y-%m-%d') + '_' +
                    df['commodity'].str.lower() + '_' +
                    df['location'].str.lower()
                )
                
                existing_data['unique_key'] = (
                    existing_data['date'].dt.strftime('%Y-%m-%d') + '_' +
                    existing_data['commodity'] + '_' +
                    existing_data['location']
                )
                
                # Find NEW records only
                new_records = df[~df['unique_key'].isin(existing_data['unique_key'])].copy()
                duplicate_count = len(df) - len(new_records)
                
                print(f"✅ Found {len(new_records)} NEW records")
                print(f"⚠️  Skipping {duplicate_count} duplicates already in database")
                
                df = new_records
            else:
                print("📭 Database is empty - all records are new")
        else:
            print("⚠️  Could not check database, will upload all records")
    
    except Exception as e:
        print(f"⚠️  Error checking database: {e}")
        print("Will proceed to upload all records")
    
    if df.empty:
        print("\n✅ No new records to upload! Database is up to date.")
        return
    
    # Show sample of NEW records
    print(f"\n📄 Sample NEW records (first 3):")
    for i in range(min(3, len(df))):
        row = df.iloc[i]
        print(f"   {i+1}. {row['date'].strftime('%Y-%m-%d')} | {row['commodity']} | {row['location']} | ₦{row['price']:,.0f}/{row['unit']}")
    
    # Confirm
    print(f"\n⚠️  Ready to upload {len(df)} NEW records")
    confirm = input("Continue? (yes/no): ").lower()
    
    if confirm not in ['yes', 'y']:
        print("❌ Cancelled")
        return
    
    # Upload NEW records only
    print("\n📤 Uploading NEW records...")
    
    # Convert to records format
    records = []
    for idx, row in df.iterrows():
        record = {
            "date": str(row['date'].strftime('%Y-%m-%d %H:%M:%S')),
            "commodity": str(row['commodity']),
            "location": str(row['location']),
            "unit": str(row['unit']),
            "price": float(row['price']),
            "source": str(row['source'])
        }
        records.append(record)
    
    # Bulk upload
    try:
        response = requests.post(
            f"{API_URL}/bulk-upload-other-sources",
            json=records,
            headers=HEADERS,
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n✅ SUCCESS! {result['message']}")
            print(f"✅ {len(records)} records uploaded successfully!")
        else:
            print(f"\n❌ Upload failed!")
            print(f"Status: {response.status_code}")
            print(f"Error: {response.text}")
    
    except Exception as e:
        print(f"\n❌ Upload error: {e}")
    
    print(f"\n{'=' * 70}")

# =====================================================
# VERIFY DATA
# =====================================================
def verify_other_sources():
    """Check what's in the other sources database"""
    
    print("\n" + "=" * 70)
    print("VERIFYING OTHER SOURCES DATABASE")
    print("=" * 70)
    
    try:
        # Fetch all records (paginated)
        all_data = []
        page = 1
        
        print("\n🔄 Fetching all records from database...")
        
        while True:
            response = requests.get(
                f"{API_URL}/other-sources",
                params={"page": page, "page_size": 1000},
                headers=HEADERS
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # Handle paginated response
                if isinstance(result, dict) and 'data' in result:
                    data = result['data']
                    pagination = result.get('pagination', {})
                    
                    all_data.extend(data)
                    print(f"   Fetched page {page}/{pagination.get('total_pages', '?')} ({len(data)} records)")
                    
                    # Check if there are more pages
                    if not pagination.get('has_next', False):
                        break
                    
                    page += 1
                elif isinstance(result, list):
                    # Old format (non-paginated)
                    all_data = result
                    break
                else:
                    break
            else:
                print(f"⚠️  API returned status {response.status_code}")
                break
        
        if all_data:
            df = pd.DataFrame(all_data)
                print(f"\n✅ Database has {len(df)} total records")
                
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    latest_date = df['date'].max()
                    oldest_date = df['date'].min()
                    print(f"📅 Date range: {oldest_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}")
                
                if 'commodity' in df.columns:
                    print(f"\n📊 Commodities ({df['commodity'].nunique()}):")
                    for comm in sorted(df['commodity'].unique()[:10]):
                        count = len(df[df['commodity'] == comm])
                        print(f"   - {comm}: {count} records")
                
                if 'location' in df.columns:
                    print(f"\n📍 Locations ({df['location'].nunique()}):")
                    for loc in sorted(df['location'].unique()[:10]):
                        count = len(df[df['location'] == loc])
                        print(f"   - {loc}: {count} records")
                
                print(f"\n📋 Latest 5 records:")
                latest = df.sort_values('date', ascending=False).head(5)
                for idx, row in latest.iterrows():
                    print(f"   {row['date']} | {row['commodity']} | {row['location']} | ₦{row['price']:,.0f}")
            else:
                print("\n⚠️  Database is empty")
        else:
            print(f"\n❌ Error: {response.status_code}")
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
    
    print("\n" + "=" * 70)

# =====================================================
# RUN
# =====================================================
if __name__ == "__main__":
    
    print("\n🌾 AGRIARCHE - OTHER SOURCES SMART UPLOADER")
    print("\nWhat would you like to do?")
    print("1. Upload ONLY NEW records (recommended)")
    print("2. Verify current database")
    print("3. Both (upload then verify)")
    
    choice = input("\nChoice (1/2/3): ").strip()
    
    if choice == "1":
        file_path = input("\nEnter Excel/CSV file path: ").strip()
        upload_new_other_sources(file_path)
    elif choice == "2":
        verify_other_sources()
    elif choice == "3":
        file_path = input("\nEnter Excel/CSV file path: ").strip()
        upload_new_other_sources(file_path)
        verify_other_sources()
    else:
        print("Invalid choice")
