"""
Upload Other Sources (External) Market Data to Agriarche Backend
==============================================================
This script uploads externally sourced market price data from Excel files with:
- Automatic commodity name standardization
- Data quality validation
- Duplicate prevention
- Bulk upload optimization

Excel Columns Expected:
- date, commodity, location, unit, price_clean

Requirements:
  pip install pandas openpyxl requests

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

BATCH_SIZE = 100

# =====================================================
# COMMODITY NAME STANDARDIZATION
# =====================================================
def standardize_commodity_name(commodity):
    """Standardize commodity names to match database conventions"""
    if not commodity or not isinstance(commodity, str):
        return commodity
    
    commodity = commodity.strip()
    
    STANDARD_NAMES = {
        'soybeans': 'Soybeans',
        'soya beans': 'Soybeans',
        'white beans (zapa)': 'White Beans (Zapa)',
        'white beans': 'White Beans (Zapa)',
        'paddy rice': 'Paddy Rice',
        'rice paddy': 'Paddy Rice',
        'maize (corn) white new harvest': 'Maize (Corn) White New Harvest',
        'maize white': 'Maize (Corn) White New Harvest',
        'millet (dauro)': 'Millet (Dauro)',
        'millet (gero)': 'Millet (Gero)',
        'millet': 'Millet (Dauro)',
        'guinea corn (dawa)': 'Guinea Corn (Dawa)',
        'chili pepper': 'Chili Pepper',
    }
    
    commodity_lower = commodity.lower()
    if commodity_lower in STANDARD_NAMES:
        return STANDARD_NAMES[commodity_lower]
    
    # Return in Title Case (Proper Case) as per database standard
    return commodity.title()  # ← Changed from .upper() to .title()


def validate_data_quality(df):
    """Validate data before uploading"""
    issues = []
    
    # Check for required fields
    required_fields = ['date', 'commodity', 'location', 'unit',]
    for field in required_fields:
        if field not in df.columns:
            issues.append(f"❌ Missing required field: {field}")
        else:
            missing_count = df[field].isna().sum()
            if missing_count > 0:
                issues.append(f"⚠️  Found {missing_count} records with missing {field}")
    
    # Check for invalid prices (works for both 'price' and 'price_clean')
    price_col = 'price' if 'price' in df.columns else 'price_clean'
    if price_col in df.columns:
        df['price_numeric'] = pd.to_numeric(df[price_col], errors='coerce')
        
        invalid_prices = df[(df['price_numeric'] <= 0) | (df['price_numeric'].isna())]
        if len(invalid_prices) > 0:
            issues.append(f"⚠️  Found {len(invalid_prices)} records with invalid prices")
    
    if issues:
        print("\n" + "="*60)
        print("🚨 DATA QUALITY ISSUES FOUND:")
        print("="*60)
        for issue in issues:
            print(f"  {issue}")
        print("="*60)
        
        critical_errors = [i for i in issues if i.startswith("❌")]
        if critical_errors:
            print("\n❌ CRITICAL ERRORS - Cannot proceed")
            return False
        
        response = input("\n❓ Continue with upload? (yes/no): ")
        if response.lower() != 'yes':
            print("❌ Upload cancelled")
            return False
    
    return True


def upload_other_sources_data(excel_file_path):
    """Upload other sources data from Excel file"""
    try:
        print(f"\n📂 Loading data from: {excel_file_path}")
        df = pd.read_excel(excel_file_path)
        
        print(f"✅ Loaded {len(df)} records")
        
        # Expected columns from your screenshot
        required_columns = ['date', 'commodity', 'location', 'unit', 'price_clean']
        
        # Check for missing columns
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            print(f"❌ Missing required columns: {missing_cols}")
            print(f"\n📋 Found columns: {df.columns.tolist()}")
            return
        
        # Keep only required columns
        df = df[required_columns].copy()
        
        # Rename price_clean to price for API
        df = df.rename(columns={'price_clean': 'price'})
        
        # ✅ STANDARDIZE COMMODITY NAMES
        print("\n🔄 Standardizing commodity names...")
        df['commodity_original'] = df['commodity'].copy()
        df['commodity'] = df['commodity'].apply(standardize_commodity_name)
        
        changes = df[df['commodity'] != df['commodity_original']][['commodity_original', 'commodity']].drop_duplicates()
        if len(changes) > 0:
            print(f"✅ Standardized {len(changes)} commodity name variations:")
            for _, row in changes.iterrows():
                print(f"   '{row['commodity_original']}' → '{row['commodity']}'")
        
        # ✅ VALIDATE DATA QUALITY
        print("\n🔍 Validating data quality...")
        if not validate_data_quality(df):
            return
        
        # ✅ CHECK FOR EXISTING RECORDS
        print("\n🔍 Checking for existing records in database...")
        try:
            existing_response = requests.get(
                f"{BASE_URL}/other-sources",
                params={"page": 1, "page_size": 10000},
                headers=HEADERS,
                timeout=15
            )
            
            if existing_response.status_code == 200:
                existing_result = existing_response.json()
                existing_data = existing_result.get('data', existing_result) if isinstance(existing_result, dict) else existing_result
                
                if existing_data:
                    existing_df = pd.DataFrame(existing_data)
                    
                    df['compare_key'] = (
                        pd.to_datetime(df['date']).astype(str) + "_" + 
                        df['commodity'].astype(str) + "_" + 
                        df['location'].astype(str)
                    )
                    
                    existing_df['compare_key'] = (
                        pd.to_datetime(existing_df['date']).astype(str) + "_" + 
                        existing_df['commodity'].astype(str) + "_" + 
                        existing_df['location'].astype(str)
                    )
                    
                    before_count = len(df)
                    df = df[~df['compare_key'].isin(existing_df['compare_key'])]
                    after_count = len(df)
                    duplicates_found = before_count - after_count
                    
                    if duplicates_found > 0:
                        print(f"⚠️  Skipping {duplicates_found} duplicate records")
                        print(f"✅ {after_count} new records to upload")
                    else:
                        print(f"✅ No duplicates - all {after_count} records are new")
                    
                    if after_count == 0:
                        print("\n✅ All records already exist - nothing to upload!")
                        return
        except Exception as e:
            print(f"⚠️  Could not check for duplicates: {str(e)}")
            print("   Proceeding with upload anyway...")
        
        # Convert to records
        records = []
        for index, row in df.iterrows():
            try:
                records.append({
                    "date": str(row['date']),
                    "commodity": str(row['commodity']),
                    "location": str(row['location']),
                    "unit": str(row['unit']),
                    "price": float(row['price'])
                })
            except Exception as e:
                print(f"⚠️  Skipping row {index}: {str(e)}")
        
        if not records:
            print("❌ No valid records to upload")
            return
        
        # Upload in batches
        print(f"\n📤 Uploading {len(records)} records in batches of {BATCH_SIZE}...")
        total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
        success_count = 0
        error_count = 0
        
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            
            try:
                response = requests.post(
                    f"{BASE_URL}/bulk-upload-other-sources",
                    json=batch,
                    headers=HEADERS,
                    timeout=30
                )
                
                if response.status_code == 200:
                    success_count += len(batch)
                    print(f"   ✓ Batch {batch_num}/{total_batches}: Uploaded {len(batch)} records")
                else:
                    error_count += len(batch)
                    print(f"   ✗ Batch {batch_num}/{total_batches} failed: {response.status_code}")
                    
            except Exception as e:
                error_count += len(batch)
                print(f"   ✗ Batch {batch_num}/{total_batches} error: {str(e)}")
        
        # Summary
        print("\n" + "="*60)
        print("📊 UPLOAD SUMMARY")
        print("="*60)
        print(f"✅ Successfully uploaded: {success_count} records")
        print(f"❌ Failed: {error_count} records")
        print(f"📈 Success rate: {(success_count/len(records)*100):.1f}%")
        print("="*60)
        
    except FileNotFoundError:
        print(f"❌ Error: File not found: {excel_file_path}")
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")


if __name__ == "__main__":
    print("="*60)
    print("🌐 AGRIARCHE - OTHER SOURCES (EXTERNAL) DATA UPLOAD")
    print("="*60)
    
    excel_file = input("\n📁 Enter Excel file path (or press Enter for default 'clean_prices.xlsx'): ").strip()
    
    if not excel_file:
        excel_file = "clean_prices.xlsx"
    
    if not os.path.exists(excel_file):
        print(f"\n❌ File not found: {excel_file}")
        print("\n💡 Make sure the Excel file is in the same folder as this script")
    else:
        try:
            preview = pd.read_excel(excel_file, nrows=3)
            print(f"\n📄 File found: {excel_file}")
            print(f"📊 Preview (first 3 rows):")
            print(preview.to_string(index=False))
            print(f"\n📈 Total rows in file: {len(pd.read_excel(excel_file))}")
        except:
            pass
        
        confirm = input("\n🚀 Ready to upload? (yes/no): ")
        
        if confirm.lower() == 'yes':
            upload_other_sources_data(excel_file)
        else:
            print("❌ Upload cancelled")
    
    print("\n✅ Script completed!")
