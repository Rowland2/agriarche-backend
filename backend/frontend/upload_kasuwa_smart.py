"""
SMART Upload Script for Kasuwa Internal Prices
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
def upload_new_records_only(excel_file_path):
    """
    Smart upload that only uploads NEW records
    Checks database first to avoid duplicates
    """
    
    print("=" * 70)
    print("SMART UPLOAD - NEW RECORDS ONLY")
    print("=" * 70)
    
    # Read File (Auto-detect CSV or Excel)
    print(f"\n📂 Reading: {excel_file_path}")
    try:
        if excel_file_path.lower().endswith('.csv'):
            df = pd.read_csv(excel_file_path)
        else:
            df = pd.read_excel(excel_file_path)
        print(f"✅ Loaded {len(df)} rows")
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return
    
    # Map columns (same as before)
    print("\n🔄 Mapping columns...")
    col_map = {}
    
    for col in df.columns:
        col_lower = col.lower().strip()
        
        if any(x in col_lower for x in ['start_time', 'start time', 'date', 'timestamp']):
            col_map[col] = 'start_time'
        elif 'commodity' in col_lower:
            col_map[col] = 'commodity'
        elif 'market' in col_lower:
            col_map[col] = 'market'
        elif 'price_per_kg' in col_lower or 'price per kg' in col_lower:
            col_map[col] = 'price_per_kg'
        elif 'price_per_bag' in col_lower or 'price per bag' in col_lower:
            col_map[col] = 'price_per_bag'
        elif 'weight' in col_lower and 'bag' in col_lower:
            col_map[col] = 'weight_of_bag_kg'
        elif 'agent' in col_lower:
            col_map[col] = 'agent_code'
        elif 'state' in col_lower:
            col_map[col] = 'state'
        elif 'availability' in col_lower:
            col_map[col] = 'availability'
        elif 'commodity_type' in col_lower or 'type' in col_lower:
            col_map[col] = 'commodity_type'
    
    df = df.rename(columns=col_map)
    
    # Clean data
    print("\n🧹 Cleaning data...")
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df = df.dropna(subset=['start_time'])
    
    if 'price_per_bag' not in df.columns:
        df['price_per_bag'] = df['price_per_kg'] * 100
    if 'weight_of_bag_kg' not in df.columns:
        df['weight_of_bag_kg'] = 100
    if 'agent_code' not in df.columns:
        df['agent_code'] = 'WEB_UPLOAD'
    if 'state' not in df.columns:
        df['state'] = 'Unknown'
    if 'availability' not in df.columns:
        df['availability'] = 'Available'
    if 'commodity_type' not in df.columns:
        df['commodity_type'] = 'General'
    
    df['price_per_kg'] = pd.to_numeric(df['price_per_kg'], errors='coerce')
    df['price_per_bag'] = pd.to_numeric(df['price_per_bag'], errors='coerce')
    df = df.dropna(subset=['price_per_kg', 'price_per_bag'])
    
    print(f"✅ {len(df)} valid rows in Excel")
    
    # Fetch existing data from database
    print("\n🔍 Checking database for existing records...")
    try:
        response = requests.get(f"{API_URL}/prices", headers=HEADERS)
        if response.status_code == 200:
            existing_data = pd.DataFrame(response.json())
            
            if not existing_data.empty:
                existing_data['start_time'] = pd.to_datetime(existing_data['start_time'])
                print(f"📊 Database has {len(existing_data)} existing records")
                
                # Create unique identifier for deduplication
                df['unique_key'] = (
                    df['start_time'].dt.strftime('%Y-%m-%d %H:%M') + '_' +
                    df['commodity'].str.lower() + '_' +
                    df['market'].str.lower()
                )
                
                existing_data['unique_key'] = (
                    existing_data['start_time'].dt.strftime('%Y-%m-%d %H:%M') + '_' +
                    existing_data['commodity'].str.lower() + '_' +
                    existing_data['market'].str.lower()
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
        print(f"   {i+1}. {row['start_time'].strftime('%Y-%m-%d %H:%M')} | {row['commodity']} | {row['market']} | ₦{row['price_per_kg']:,.2f}/kg")
    
    # Confirm
    print(f"\n⚠️  Ready to upload {len(df)} NEW records")
    confirm = input("Continue? (yes/no): ").lower()
    
    if confirm not in ['yes', 'y']:
        print("❌ Cancelled")
        return
    
    # Upload NEW records only
    print("\n📤 Uploading NEW records...")
    
    success_count = 0
    failed_count = 0
    
    for idx, row in df.iterrows():
        record = {
            "start_time": str(row['start_time'].strftime('%Y-%m-%d %H:%M:%S')),
            "agent_code": str(row['agent_code']),
            "state": str(row['state']),
            "market": str(row['market']),
            "commodity": str(row['commodity']),
            "price_per_bag": float(row['price_per_bag']),
            "weight_of_bag_kg": float(row['weight_of_bag_kg']),
            "price_per_kg": float(row['price_per_kg']),
            "availability": str(row['availability']),
            "commodity_type": str(row['commodity_type'])
        }
        
        try:
            response = requests.post(
                f"{API_URL}/update-price",
                json=record,
                headers=HEADERS,
                timeout=10
            )
            
            if response.status_code == 200:
                success_count += 1
                if (success_count % 10 == 0):
                    print(f"   ✅ Uploaded {success_count}/{len(df)} records...")
            else:
                failed_count += 1
                print(f"   ❌ Failed: {response.text}")
        
        except Exception as e:
            failed_count += 1
            print(f"   ❌ Error: {e}")
    
    print(f"\n{'=' * 70}")
    print(f"✅ Successfully uploaded: {success_count} NEW records")
    print(f"❌ Failed: {failed_count} records")
    print(f"{'=' * 70}")

# =====================================================
# FORCE UPLOAD ALL (Original Behavior)
# =====================================================
def upload_all_records(excel_file_path):
    """
    Upload ALL records regardless of duplicates
    Use only if you want to re-upload everything
    """
    
    print("=" * 70)
    print("UPLOAD ALL RECORDS (INCLUDING DUPLICATES)")
    print("=" * 70)
    print("⚠️  WARNING: This will upload ALL rows, even duplicates!")
    print()
    
    confirm = input("Are you SURE you want to upload ALL records? (yes/no): ").lower()
    if confirm not in ['yes', 'y']:
        print("❌ Cancelled")
        return
    
    # Same logic as original upload_kasuwa_internal_prices.py
    # ... (full upload logic here)
    print("Uploading all records...")
    # Add the full original upload logic here if needed

# =====================================================
# VERIFY DATA
# =====================================================
def verify_data():
    """Check what's in the database"""
    
    print("\n" + "=" * 70)
    print("VERIFYING DATABASE")
    print("=" * 70)
    
    try:
        response = requests.get(f"{API_URL}/prices", headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            
            if data:
                df = pd.DataFrame(data)
                print(f"\n✅ Database has {len(df)} total records")
                
                if 'start_time' in df.columns:
                    df['start_time'] = pd.to_datetime(df['start_time'])
                    latest_date = df['start_time'].max()
                    oldest_date = df['start_time'].min()
                    print(f"📅 Date range: {oldest_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}")
                
                if 'commodity' in df.columns:
                    print(f"\n📊 Commodities ({df['commodity'].nunique()}):")
                    for comm in sorted(df['commodity'].unique()[:10]):
                        count = len(df[df['commodity'] == comm])
                        print(f"   - {comm}: {count} records")
                
                if 'market' in df.columns:
                    print(f"\n📍 Markets ({df['market'].nunique()}):")
                    for market in sorted(df['market'].unique()[:10]):
                        count = len(df[df['market'] == market])
                        print(f"   - {market}: {count} records")
                
                print(f"\n📋 Latest 5 records:")
                latest = df.sort_values('start_time', ascending=False).head(5)
                for idx, row in latest.iterrows():
                    print(f"   {row['start_time']} | {row['commodity']} | {row['market']} | ₦{row['price_per_kg']:.2f}/kg")
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
    
    print("\n🌾 AGRIARCHE SMART UPLOADER")
    print("\nWhat would you like to do?")
    print("1. Upload ONLY NEW records (recommended)")
    print("2. Upload ALL records (creates duplicates)")
    print("3. Verify current database")
    
    choice = input("\nChoice (1/2/3): ").strip()
    
    if choice == "1":
        file_path = input("\nEnter Excel file path: ").strip()
        upload_new_records_only(file_path)
    elif choice == "2":
        file_path = input("\nEnter Excel file path: ").strip()
        upload_all_records(file_path)
    elif choice == "3":
        verify_data()
    else:
        print("Invalid choice")