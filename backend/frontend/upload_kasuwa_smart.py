"""
Upload Kasuwa Internal Price Data to Agriarche Backend
=====================================================
This script uploads Kasuwa internal market price data from CSV files with:
- Automatic commodity name standardization
- Data quality validation
- Duplicate prevention
- Bad price detection

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
        'groundut kampala': 'Groundnut Kampala',  # Typo fix
        
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
        'rice  processed': 'Processed Rice',  # Double space
        
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
        
        # Honey Beans variations
        'honey beans': 'Honey Beans',
        'honeybeans': 'Honey Beans',
        
        # Millet variations
        'millet (gero)': 'Millet (Gero)',
        'millet (dauro)': 'Millet (Dauro)',
        'millet': 'Millet',
        
        # Chili Pepper
        'chili pepper': 'Chili Pepper',
        
        # Other commodities
        'groundnuts (peanuts)': 'Groundnuts (Peanuts)',
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
    
    # Check for suspicious prices in specific markets
    if 'market' in df.columns and 'price_per_kg' in df.columns:
        # Flag Pambegua records with price < 500
        df['price_per_kg_numeric'] = pd.to_numeric(df['price_per_kg'], errors='coerce')
        
        pambegua_low = df[
            (df['market'] == 'Pambegua') & 
            (df['price_per_kg_numeric'] < 500) &
            (df['price_per_kg_numeric'] > 0)
        ]
        
        if len(pambegua_low) > 0:
            issues.append(f"⚠️  Found {len(pambegua_low)} Pambegua records with price < ₦500")
            print("\n🔍 Suspicious Pambegua prices:")
            print(pambegua_low[['commodity', 'market', 'price_per_kg', 'start_time']])
    
    # Check for ALL CAPS commodities
    if 'commodity' in df.columns:
        all_caps = df[df['commodity'].str.isupper() & (df['commodity'].str.len() > 2)]
        if len(all_caps) > 0:
            issues.append(f"⚠️  Found {len(all_caps)} ALL CAPS commodities (will be auto-fixed)")
            print("\n🔍 ALL CAPS commodities found:")
            print(all_caps['commodity'].unique())
    
    # Check for very low prices (potential data entry errors)
    if 'price_per_kg' in df.columns:
        df['price_per_kg_numeric'] = pd.to_numeric(df['price_per_kg'], errors='coerce')
        very_low = df[
            (df['price_per_kg_numeric'] < 50) & 
            (df['price_per_kg_numeric'] > 0)
        ]
        
        if len(very_low) > 0:
            issues.append(f"⚠️  Found {len(very_low)} records with price < ₦50/kg (possible error)")
            print("\n🔍 Very low prices (< ₦50/kg):")
            print(very_low[['commodity', 'market', 'price_per_kg']])
    
    # Check for duplicate entries
    if 'start_time' in df.columns and 'commodity' in df.columns and 'market' in df.columns:
        duplicates = df[df.duplicated(subset=['start_time', 'commodity', 'market'], keep=False)]
        if len(duplicates) > 0:
            issues.append(f"⚠️  Found {len(duplicates)} potential duplicate records")
            print("\n🔍 Duplicate records:")
            print(duplicates[['start_time', 'commodity', 'market', 'price_per_kg']])
    
    # Report issues
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


# =====================================================
# UPLOAD FUNCTION
# =====================================================
def upload_kasuwa_data(csv_file_path):
    """
    Upload Kasuwa internal price data from CSV file
    
    Args:
        csv_file_path (str): Path to CSV file
    """
    try:
        # Load CSV
        print(f"\n📂 Loading data from: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        
        print(f"✅ Loaded {len(df)} records")
        
        # Required columns
        required_columns = [
            'start_time', 'agent_code', 'state', 'market', 'commodity',
            'price_per_bag', 'weight_of_bag_kg', 'price_per_kg', 
            'availability', 'commodity_type'
        ]
        
        # Check for missing columns
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            print(f"❌ Missing required columns: {missing_cols}")
            return
        
        # ✅ STANDARDIZE COMMODITY NAMES
        print("\n🔄 Standardizing commodity names...")
        df['commodity_original'] = df['commodity'].copy()  # Keep original for reference
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
            return  # User cancelled
        
        # ✅ CHECK FOR EXISTING RECORDS (PREVENT DUPLICATES)
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
                    
                    # Create comparison keys
                    df['compare_key'] = (
                        df['start_time'].astype(str) + "_" + 
                        df['commodity'].astype(str) + "_" + 
                        df['market'].astype(str)
                    )
                    
                    existing_df['compare_key'] = (
                        existing_df['start_time'].astype(str) + "_" + 
                        existing_df['commodity'].astype(str) + "_" + 
                        existing_df['market'].astype(str)
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
        
        # Upload each record
        print(f"\n📤 Uploading {len(df)} records to backend...")
        success_count = 0
        error_count = 0
        skipped_count = 0
        
        for index, row in df.iterrows():
            try:
                data = {
                    "start_time": str(row['start_time']),
                    "agent_code": str(row['agent_code']),
                    "state": str(row['state']),
                    "market": str(row['market']),
                    "commodity": str(row['commodity']),  # Already standardized
                    "price_per_bag": float(row['price_per_bag']),
                    "weight_of_bag_kg": float(row['weight_of_bag_kg']),
                    "price_per_kg": float(row['price_per_kg']),
                    "availability": str(row['availability']),
                    "commodity_type": str(row['commodity_type'])
                }
                
                response = requests.post(
                    f"{BASE_URL}/update-price",
                    json=data,
                    headers=HEADERS,
                    timeout=10
                )
                
                if response.status_code == 200:
                    success_count += 1
                    if (index + 1) % 10 == 0:
                        print(f"   ✓ Uploaded {index + 1}/{len(df)} records...")
                elif response.status_code == 409:  # Conflict - duplicate
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
        print(f"📈 Success rate: {(success_count/(len(df))*100):.1f}%")
        print("="*60)
        
    except FileNotFoundError:
        print(f"❌ Error: File not found: {csv_file_path}")
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")


# =====================================================
# MAIN EXECUTION
# =====================================================
if __name__ == "__main__":
    print("="*60)
    print("🌾 AGRIARCHE - KASUWA INTERNAL DATA UPLOAD")
    print("="*60)
    
    # Get file path from user
    csv_file = input("\n📁 Enter CSV file path (or press Enter for default 'kasuwa_data.csv'): ").strip()
    
    if not csv_file:
        csv_file = "kasuwa_data.csv"
    
    # Check if file exists
    if not os.path.exists(csv_file):
        print(f"\n❌ File not found: {csv_file}")
        print("\n💡 Make sure the CSV file is in the same folder as this script,")
        print("   or provide the full path (e.g., C:/Users/YourName/Documents/kasuwa_data.csv)")
    else:
        # Confirm upload
        print(f"\n📄 File found: {csv_file}")
        confirm = input("🚀 Ready to upload? (yes/no): ")
        
        if confirm.lower() == 'yes':
            upload_kasuwa_data(csv_file)
        else:
            print("❌ Upload cancelled")
    
    print("\n✅ Script completed!")
