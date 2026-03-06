"""
Upload Kasuwa Internal Price Data to Agriarche Backend
=====================================================
This script uploads Kasuwa internal market price data from CSV files with:
- Automatic commodity name standardization
- Data quality validation
- Duplicate prevention
- Bad price detection

CSV Columns Expected:
- Start Time, Agent Code, State, Market, Commodity, 
- Price per Bag, Weight of Bag (kg), price_per_kg, Availability

Requirements:
  pip install pandas requests

Author: Agriarche Team
Last Updated: March 2026
"""

import pandas as pd
import requests
import os
from datetime import datetime

# =====================================================
# CONFIGURATION
# =====================================================
BASE_URL = "https://agriarche-backend.onrender.com"
API_KEY = "Agriarche_Internal_Key_2026"
HEADERS = {"access_token": API_KEY}

# =====================================================
# COMMODITY NAME STANDARDIZATION
# =====================================================
def standardize_commodity_name(commodity):
    """
    Standardize commodity names to match database conventions
    """
    if not commodity or not isinstance(commodity, str):
        return commodity
    
    commodity = commodity.strip()
    
    STANDARD_NAMES = {
        # Soybeans
        'soya beans': 'Soybeans',
        'soya bean': 'Soybeans',
        'soy beans': 'Soybeans',
        'soybeans': 'Soybeans',
        
        # Groundnut
        'groundnut gargaja': 'Groundnut Gargaja',
        'groundnut kampala': 'Groundnut Kampala',
        
        # White Beans
        'white beans': 'White Beans (Zapa)',
        'white beans (zapa)': 'White Beans (Zapa)',
        'white beans (misra)': 'White Beans (Misra)',
        
        # Rice
        'rice paddy': 'Paddy Rice',
        'paddy rice': 'Paddy Rice',
        'rice processed': 'Rice Processed',
        'processed rice': 'Rice Processed',
        
        # Cowpea
        'cowpea white': 'Cowpea White',
        'cowpea brown': 'Cowpea Brown',
        
        # Maize
        'maize white': 'Maize White',
        'maize': 'Maize',
        
        # Sorghum
        'sorghum red': 'Sorghum Red',
        'sorghum white': 'Sorghum White',
        'sorghum yellow': 'Sorghum Yellow',
        'sorghum': 'Sorghum',
        
        # Honey Beans
        'honey beans': 'Honey Beans',
        'honeybeans': 'Honey Beans',
        
        # Millet
        'millet': 'Millet',
    }
    
    commodity_lower = commodity.lower()
    if commodity_lower in STANDARD_NAMES:
        return STANDARD_NAMES[commodity_lower]
    
    return commodity.title()


def validate_data_quality(df):
    """Validate data before uploading"""
    issues = []
    
    # Check for suspicious prices
    if 'price_per_kg' in df.columns:
        df['price_per_kg_numeric'] = pd.to_numeric(df['price_per_kg'], errors='coerce')
        
        pambegua_low = df[
            (df['Market'] == 'Pambegua') & 
            (df['price_per_kg_numeric'] < 500) &
            (df['price_per_kg_numeric'] > 0)
        ]
        
        if len(pambegua_low) > 0:
            issues.append(f"⚠️  Found {len(pambegua_low)} Pambegua records with price < ₦500")
    
    # Check for ALL CAPS
    if 'Commodity' in df.columns:
        all_caps = df[df['Commodity'].str.isupper() & (df['Commodity'].str.len() > 2)]
        if len(all_caps) > 0:
            issues.append(f"⚠️  Found {len(all_caps)} ALL CAPS commodities (will be auto-fixed)")
    
    if issues:
        print("\n" + "="*60)
        print("🚨 DATA QUALITY ISSUES FOUND:")
        print("="*60)
        for issue in issues:
            print(f"  {issue}")
        print("="*60)
        
        response = input("\n❓ Continue with upload? (yes/no): ")
        if response.lower() != 'yes':
            print("❌ Upload cancelled by user")
            return False
    
    return True


def upload_kasuwa_data(csv_file_path):
    """Upload Kasuwa internal price data from CSV file"""
    try:
        print(f"\n📂 Loading data from: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        
        print(f"✅ Loaded {len(df)} records")
        
        # Map CSV column names to API field names
        column_mapping = {
            'Start Time': 'start_time',
            'Agent Code': 'agent_code',
            'State': 'state',
            'Market': 'market',
            'Commodity': 'commodity',
            'Price per Bag': 'price_per_bag',
            'Weight of Bag (kg)': 'weight_of_bag_kg',
            'price_per_kg': 'price_per_kg',
            'Availability': 'availability'
        }
        
        # Check for required columns
        missing_cols = [col for col in column_mapping.keys() if col not in df.columns]
        if missing_cols:
            print(f"❌ Missing required columns: {missing_cols}")
            print(f"📋 Found columns: {df.columns.tolist()}")
            return
        
        # Rename columns
        df_clean = df[column_mapping.keys()].copy()
        df_clean.columns = column_mapping.values()
        
        # ✅ STANDARDIZE COMMODITY NAMES
        print("\n🔄 Standardizing commodity names...")
        df_clean['commodity_original'] = df_clean['commodity'].copy()
        df_clean['commodity'] = df_clean['commodity'].apply(standardize_commodity_name)
        
        changes = df_clean[df_clean['commodity'] != df_clean['commodity_original']][['commodity_original', 'commodity']].drop_duplicates()
        if len(changes) > 0:
            print(f"✅ Standardized {len(changes)} commodity name variations:")
            for _, row in changes.iterrows():
                print(f"   '{row['commodity_original']}' → '{row['commodity']}'")
        
        # ✅ CHECK FOR EXISTING RECORDS (DO THIS FIRST)
        print("\n🔍 Checking for existing records in database...")
        try:
            existing_response = requests.get(
                f"{BASE_URL}/prices",
                params={"page": 1, "page_size": 10000},
                headers=HEADERS,
                timeout=15
            )
            
            if existing_response.status_code == 200:
                existing_result = existing_response.json()
                existing_data = existing_result.get('data', existing_result) if isinstance(existing_result, dict) else existing_result
                
                if existing_data:
                    existing_df = pd.DataFrame(existing_data)
                    existing_df['start_time'] = pd.to_datetime(existing_df['start_time']).astype(str)
                    
                    df_clean['compare_key'] = (
                        pd.to_datetime(df_clean['start_time']).astype(str) + "_" + 
                        df_clean['commodity'].astype(str) + "_" + 
                        df_clean['market'].astype(str)
                    )
                    
                    existing_df['compare_key'] = (
                        existing_df['start_time'].astype(str) + "_" + 
                        existing_df['commodity'].astype(str) + "_" + 
                        existing_df['market'].astype(str)
                    )
                    
                    before_count = len(df_clean)
                    df_clean = df_clean[~df_clean['compare_key'].isin(existing_df['compare_key'])]
                    after_count = len(df_clean)
                    duplicates_found = before_count - after_count
                    
                    if duplicates_found > 0:
                        print(f"⚠️  Skipping {duplicates_found} duplicate records already in database")
                        print(f"✅ {after_count} new records to upload")
                    else:
                        print(f"✅ No duplicates found - all {after_count} records are new")
                    
                    if after_count == 0:
                        print("\n✅ All records already exist in database - nothing to upload!")
                        return
                else:
                    print("✅ Database is empty - uploading all records")
            else:
                print(f"⚠️  Could not check for duplicates (API status: {existing_response.status_code})")
                print("   Proceeding with upload anyway...")
                
        except Exception as e:
            print(f"⚠️  Could not check for duplicates: {str(e)}")
            print("   Proceeding with upload anyway...")
        
        # ✅ VALIDATE DATA QUALITY (AFTER DUPLICATE CHECK - only validates NEW records)
        print("\n🔍 Validating data quality...")
        if not validate_data_quality(df_clean):
            return
        
        # Upload each record
        print(f"\n📤 Uploading {len(df_clean)} records to backend...")
        success_count = 0
        error_count = 0
        skipped_count = 0
        
        for index, row in df_clean.iterrows():
            try:
                data = {
                    "start_time": str(row['start_time']),
                    "agent_code": str(row['agent_code']),
                    "state": str(row['state']),
                    "market": str(row['market']),
                    "commodity": str(row['commodity']),
                    "price_per_bag": float(row['price_per_bag']),
                    "weight_of_bag_kg": float(row['weight_of_bag_kg']),
                    "price_per_kg": float(row['price_per_kg']),
                    "availability": str(row['availability']),
                    "commodity_type": "Grains"  # Default value
                }
                
                response = requests.post(
                    f"{BASE_URL}/update-price",
                    json=data,
                    headers=HEADERS,
                    timeout=10
                )
                
                if response.status_code == 200:
                    success_count += 1
                    if (success_count) % 10 == 0:
                        print(f"   ✓ Uploaded {success_count}/{len(df_clean)} records...")
                elif response.status_code == 409:
                    skipped_count += 1
                else:
                    error_count += 1
                    print(f"   ✗ Error on record {index + 1}: {response.status_code} - {response.text}")
                    
            except Exception as e:
                error_count += 1
                print(f"   ✗ Error on record {index + 1}: {str(e)}")
        
        # Summary
        print("\n" + "="*60)
        print("📊 UPLOAD SUMMARY")
        print("="*60)
        print(f"✅ Successfully uploaded: {success_count} records")
        print(f"⏭️  Skipped (duplicates): {skipped_count} records")
        print(f"❌ Failed: {error_count} records")
        print(f"📈 Success rate: {(success_count/(len(df_clean))*100):.1f}%")
        print("="*60)
        
    except FileNotFoundError:
        print(f"❌ Error: File not found: {csv_file_path}")
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")


if __name__ == "__main__":
    print("="*60)
    print("🌾 AGRIARCHE - KASUWA INTERNAL DATA UPLOAD")
    print("="*60)
    
    csv_file = input("\n📁 Enter CSV file path (or press Enter for default 'prices.csv'): ").strip()
    
    if not csv_file:
        csv_file = "prices.csv"
    
    if not os.path.exists(csv_file):
        print(f"\n❌ File not found: {csv_file}")
        print("\n💡 Make sure the CSV file is in the same folder as this script")
    else:
        print(f"\n📄 File found: {csv_file}")
        confirm = input("🚀 Ready to upload? (yes/no): ")
        
        if confirm.lower() == 'yes':
            upload_kasuwa_data(csv_file)
        else:
            print("❌ Upload cancelled")
    
    print("\n✅ Script completed!")
