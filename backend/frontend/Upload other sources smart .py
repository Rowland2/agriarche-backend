"""
Upload Other Sources (External) Market Data to Agriarche Backend
==============================================================
This script uploads externally sourced market price data from Excel files with:
- Automatic commodity name standardization
- Data quality validation
- Duplicate prevention
- Bulk upload optimization

Requirements:
  pip install pandas openpyxl requests

Supported formats: .xlsx, .xls

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

# Bulk upload batch size
BATCH_SIZE = 100

# =====================================================
# COMMODITY NAME STANDARDIZATION
# =====================================================
def standardize_commodity_name(commodity):
    """
    Standardize commodity names to match database conventions
    This prevents duplicates from inconsistent naming
    
    Args:
        commodity (str): Raw commodity name from CSV
        
    Returns:
        str: Standardized commodity name
    """
    if not commodity or not isinstance(commodity, str):
        return commodity
    
    # Trim whitespace
    commodity = commodity.strip()
    
    # Define standard mappings (lowercase → proper case)
    STANDARD_NAMES = {
        # Soybeans variations
        'soya beans': 'Soybeans',
        'soya bean': 'Soybeans',
        'soy beans': 'Soybeans',
        'soy bean': 'Soybeans',
        'soybean': 'Soybeans',
        'soybeans': 'Soybeans',
        
        # Groundnut variations
        'groundnut gargaja': 'Groundnut Gargaja',
        'groundnut kampala': 'Groundnut Kampala',
        'groundut kampala': 'Groundnut Kampala',
        
        # White Beans variations
        'white beans': 'White Beans (Zapa)',
        'white beans (zapa)': 'White Beans (Zapa)',
        'white beans zapa': 'White Beans (Zapa)',
        'white beans (misra)': 'White Beans (Misra)',
        'white beans misra': 'White Beans (Misra)',
        
        # Rice variations
        'rice paddy': 'Paddy Rice',
        'paddy rice': 'Paddy Rice',
        'rice processed': 'Processed Rice',
        'processed rice': 'Processed Rice',
        
        # Cowpea variations
        'brown cowpea': 'Cowpea Brown',
        'cowpea brown': 'Cowpea Brown',
        'white cowpea': 'Cowpea White',
        'cowpea white': 'Cowpea White',
        
        # Maize variations
        'white maize': 'Maize White',
        'maize white': 'Maize White',
        'maize (corn) white': 'Maize White',
        'maize (corn) white new harvest': 'Maize (Corn) White New Harvest',
        
        # Sorghum variations
        'red sorghum': 'Sorghum Red',
        'sorghum red': 'Sorghum Red',
        'white sorghum': 'Sorghum White',
        'sorghum white': 'Sorghum White',
        'yellow sorghum': 'Sorghum Yellow',
        'sorghum yellow': 'Sorghum Yellow',
        
        # Honey Beans
        'honey beans': 'Honey Beans',
        'honeybeans': 'Honey Beans',
        
        # Millet
        'millet (gero)': 'Millet (Gero)',
        'millet': 'Millet',
        
        # Chili Pepper
        'chili pepper': 'Chili Pepper',
        
        # Wheat
        'wheat': 'Wheat',
    }
    
    # Try exact match (case-insensitive)
    commodity_lower = commodity.lower()
    if commodity_lower in STANDARD_NAMES:
        return STANDARD_NAMES[commodity_lower]
    
    # If no exact match, use title case as fallback
    return commodity.title()


# =====================================================
# DATA QUALITY VALIDATION
# =====================================================
def validate_data_quality(df):
    """
    Validate data before uploading to prevent bad data
    
    Args:
        df (DataFrame): Data to validate
        
    Returns:
        bool: True if validation passes, False otherwise
    """
    issues = []
    
    # Check for ALL CAPS commodities
    if 'commodity' in df.columns:
        all_caps = df[df['commodity'].str.isupper() & (df['commodity'].str.len() > 2)]
        if len(all_caps) > 0:
            issues.append(f"⚠️  Found {len(all_caps)} ALL CAPS commodities (will be auto-fixed)")
            print("\n🔍 ALL CAPS commodities found:")
            print(all_caps['commodity'].unique())
    
    # Check for missing required fields
    required_fields = ['date', 'commodity', 'location', 'unit', 'price']
    for field in required_fields:
        if field not in df.columns:
            issues.append(f"❌ Missing required field: {field}")
        else:
            missing_count = df[field].isna().sum()
            if missing_count > 0:
                issues.append(f"⚠️  Found {missing_count} records with missing {field}")
    
    # Check for invalid prices
    if 'price' in df.columns:
        df['price_numeric'] = pd.to_numeric(df['price'], errors='coerce')
        
        # Negative or zero prices
        invalid_prices = df[
            (df['price_numeric'] <= 0) | 
            (df['price_numeric'].isna())
        ]
        if len(invalid_prices) > 0:
            issues.append(f"⚠️  Found {len(invalid_prices)} records with invalid prices (≤ 0 or not numeric)")
        
        # Very low prices (< ₦10 per kg or < ₦1000 per bag)
        if 'unit' in df.columns:
            very_low_kg = df[
                (df['unit'].str.lower() == 'kg') & 
                (df['price_numeric'] < 10) &
                (df['price_numeric'] > 0)
            ]
            very_low_bag = df[
                (df['unit'].str.lower() == 'bag') & 
                (df['price_numeric'] < 1000) &
                (df['price_numeric'] > 0)
            ]
            
            if len(very_low_kg) > 0:
                issues.append(f"⚠️  Found {len(very_low_kg)} records with price < ₦10/kg (possible error)")
            if len(very_low_bag) > 0:
                issues.append(f"⚠️  Found {len(very_low_bag)} records with price < ₦1000/bag (possible error)")
    
    # Check for duplicate entries
    if all(col in df.columns for col in ['date', 'commodity', 'location']):
        duplicates = df[df.duplicated(subset=['date', 'commodity', 'location'], keep=False)]
        if len(duplicates) > 0:
            issues.append(f"⚠️  Found {len(duplicates)} potential duplicate records")
    
    # Report issues
    if issues:
        print("\n" + "="*60)
        print("🚨 DATA QUALITY ISSUES FOUND:")
        print("="*60)
        for issue in issues:
            print(f"  {issue}")
        print("="*60)
        
        # Check if there are critical errors (missing required fields)
        critical_errors = [i for i in issues if i.startswith("❌")]
        if critical_errors:
            print("\n❌ CRITICAL ERRORS - Cannot proceed with upload")
            return False
        
        response = input("\n❓ Continue with upload? (yes/no): ")
        if response.lower() != 'yes':
            print("❌ Upload cancelled by user")
            return False
    
    return True


# =====================================================
# UPLOAD FUNCTION
# =====================================================
def upload_other_sources_data(excel_file_path):
    """
    Upload other sources data from Excel file using bulk upload
    
    Args:
        excel_file_path (str): Path to Excel file (.xlsx or .xls)
    """
    try:
        # Load Excel
        print(f"\n📂 Loading data from: {excel_file_path}")
        df = pd.read_excel(excel_file_path)
        
        print(f"✅ Loaded {len(df)} records")
        
        # Required columns
        required_columns = ['date', 'commodity', 'location', 'unit', 'price']
        
        # Check for missing columns
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            print(f"❌ Missing required columns: {missing_cols}")
            print(f"\n📋 Expected columns: {required_columns}")
            print(f"📋 Found columns: {df.columns.tolist()}")
            return
        
        # ✅ STANDARDIZE COMMODITY NAMES
        print("\n🔄 Standardizing commodity names...")
        df['commodity_original'] = df['commodity'].copy()
        df['commodity'] = df['commodity'].apply(standardize_commodity_name)
        
        # Show what was changed
        changes = df[df['commodity'] != df['commodity_original']][['commodity_original', 'commodity']].drop_duplicates()
        if len(changes) > 0:
            print(f"✅ Standardized {len(changes)} commodity name variations:")
            for _, row in changes.iterrows():
                print(f"   '{row['commodity_original']}' → '{row['commodity']}'")
        
        # ✅ VALIDATE DATA QUALITY
        print("\n🔍 Validating data quality...")
        if not validate_data_quality(df):
            return  # User cancelled or critical errors
        
        # ✅ CHECK FOR EXISTING RECORDS (PREVENT DUPLICATES)
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
                    
                    # Create comparison keys (date + commodity + location)
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
                    
                    # Filter out duplicates
                    before_count = len(df)
                    df = df[~df['compare_key'].isin(existing_df['compare_key'])]
                    after_count = len(df)
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
        
        # Convert DataFrame to list of records
        records = []
        for index, row in df.iterrows():
            try:
                records.append({
                    "date": str(row['date']),
                    "commodity": str(row['commodity']),  # Already standardized
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
                    print(f"   ✗ Batch {batch_num}/{total_batches} failed: {response.status_code} - {response.text}")
                    
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


# =====================================================
# MAIN EXECUTION
# =====================================================
if __name__ == "__main__":
    print("="*60)
    print("🌐 AGRIARCHE - OTHER SOURCES (EXTERNAL) DATA UPLOAD")
    print("="*60)
    
    # Get file path from user
    excel_file = input("\n📁 Enter Excel file path (or press Enter for default 'other_sources_data.xlsx'): ").strip()
    
    if not excel_file:
        excel_file = "other_sources_data.xlsx"
    
    # Check if file exists
    if not os.path.exists(excel_file):
        print(f"\n❌ File not found: {excel_file}")
        print("\n💡 Make sure the Excel file is in the same folder as this script,")
        print("   or provide the full path (e.g., C:/Users/YourName/Documents/other_sources_data.xlsx)")
        print("\n📋 Expected Excel columns:")
        print("   date, commodity, location, unit, price")
        print("\n📋 Supported formats: .xlsx, .xls")
    else:
        # Show file preview
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
