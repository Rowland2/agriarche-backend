"""
Upload Script for Other Sources Data (Excel File)
This script reads your Excel file and uploads it to Neon database via the API
"""

import pandas as pd
import requests
from datetime import datetime
import os

# =====================================================
# CONFIGURATION
# =====================================================
API_BASE_URL = "https://agriarche-backend.onrender.com"
API_KEY = "Agriarche_Internal_Key_2026"
HEADERS = {"access_token": API_KEY}

# =====================================================
# MAIN UPLOAD FUNCTION
# =====================================================
def upload_excel_to_database(excel_file_path):
    """
    Upload your Excel file containing other sources data to Neon database
    
    Expected Excel columns (can be in any order):
    - Date / date / timestamp (date column)
    - Commodity / commodity (commodity name)
    - Location / location (market location)
    - unit / Unit (e.g., "bag", "kg")
    - Price / price / Price (₦) (price value)
    
    The script will automatically map column names.
    """
    
    print("=" * 70)
    print("AGRIARCHE - OTHER SOURCES DATA UPLOADER")
    print("=" * 70)
    
    # Check if file exists
    if not os.path.exists(excel_file_path):
        print(f"❌ Error: File not found at {excel_file_path}")
        return
    
    # Read Excel file
    print(f"\n📂 Reading Excel file: {excel_file_path}")
    try:
        df = pd.read_excel(excel_file_path)
        print(f"✅ Successfully loaded {len(df)} rows")
        print(f"📊 Columns found: {df.columns.tolist()}")
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        return
    
    # Map column names (handle different possible column names)
    column_mapping = {}
    
    for col in df.columns:
        col_lower = col.lower().strip()
        
        # Date column
        if any(x in col_lower for x in ['date', 'time', 'timestamp', 'scraped']):
            column_mapping[col] = 'date'
        
        # Commodity column
        elif 'commodity' in col_lower:
            column_mapping[col] = 'commodity'
        
        # Location column
        elif any(x in col_lower for x in ['location', 'market', 'place']):
            column_mapping[col] = 'location'
        
        # Unit column
        elif 'unit' in col_lower:
            column_mapping[col] = 'unit'
        
        # Price column
        elif any(x in col_lower for x in ['price', 'amount', 'cost']):
            column_mapping[col] = 'price'
    
    # Rename columns
    df = df.rename(columns=column_mapping)
    
    print(f"\n📋 Column mapping:")
    for old_col, new_col in column_mapping.items():
        print(f"   {old_col} → {new_col}")
    
    # Verify required columns exist
    required_columns = ['date', 'commodity', 'location', 'unit', 'price']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"\n❌ Error: Missing required columns: {missing_columns}")
        print(f"Available columns after mapping: {df.columns.tolist()}")
        return
    
    # Clean and prepare data
    print(f"\n🧹 Cleaning data...")
    
    # Convert date to proper format
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Remove rows with invalid dates
    before_count = len(df)
    df = df.dropna(subset=['date'])
    after_count = len(df)
    if before_count != after_count:
        print(f"   ⚠️  Removed {before_count - after_count} rows with invalid dates")
    
    # Format date as string
    df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Clean commodity names
    df['commodity'] = df['commodity'].astype(str).str.strip()
    
    # Clean location names
    df['location'] = df['location'].astype(str).str.strip()
    
    # Clean unit
    df['unit'] = df['unit'].astype(str).str.strip()
    
    # Convert price to numeric and remove any non-numeric characters
    df['price'] = df['price'].astype(str).str.replace(',', '').str.replace('₦', '').str.strip()
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    # Remove rows with invalid prices
    before_count = len(df)
    df = df.dropna(subset=['price'])
    after_count = len(df)
    if before_count != after_count:
        print(f"   ⚠️  Removed {before_count - after_count} rows with invalid prices")
    
    print(f"✅ Data cleaned. {len(df)} valid rows ready for upload")
    
    # Convert to records format for API
    records = []
    for idx, row in df.iterrows():
        record = {
            "date": str(row['date']),
            "commodity": str(row['commodity']),
            "location": str(row['location']),
            "unit": str(row['unit']),
            "price": float(row['price'])
        }
        records.append(record)
    
    # Show sample data
    print(f"\n📄 Sample data (first 3 rows):")
    for i, record in enumerate(records[:3], 1):
        print(f"   {i}. {record['commodity']} at {record['location']} - ₦{record['price']:,.0f}/{record['unit']}")
    
    # Confirm upload
    print(f"\n⚠️  About to upload {len(records)} records to the database.")
    confirmation = input("Continue? (yes/no): ").lower().strip()
    
    if confirmation not in ['yes', 'y']:
        print("❌ Upload cancelled by user")
        return
    
    # Upload to database via API
    print(f"\n📤 Uploading to database...")
    
    try:
        response = requests.post(
            f"{API_BASE_URL}/bulk-upload-other-sources",
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
            print(f"Status code: {response.status_code}")
            print(f"Response: {response.text}")
    
    except requests.exceptions.Timeout:
        print("\n❌ Upload timed out. The file might be too large.")
        print("Try uploading in smaller batches.")
    except Exception as e:
        print(f"\n❌ Upload error: {e}")
    
    print("\n" + "=" * 70)

# =====================================================
# VERIFY UPLOADED DATA
# =====================================================
def verify_uploaded_data():
    """Check what data is currently in the database"""
    
    print("\n" + "=" * 70)
    print("VERIFYING UPLOADED DATA")
    print("=" * 70)
    
    try:
        response = requests.get(f"{API_BASE_URL}/other-sources", headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            
            if data:
                df = pd.DataFrame(data)
                print(f"\n✅ Database currently has {len(df)} records")
                print(f"\n📊 Summary:")
                print(f"   Unique Commodities: {df['commodity'].nunique()}")
                print(f"   Unique Locations: {df['location'].nunique()}")
                print(f"   Date Range: {df['date'].min()} to {df['date'].max()}")
                
                print(f"\n📋 Commodities: {', '.join(df['commodity'].unique()[:10])}")
                print(f"📍 Locations: {', '.join(df['location'].unique()[:10])}")
                
                print(f"\n📄 Sample records:")
                sample = df[['date', 'commodity', 'location', 'unit', 'price']].head(5)
                print(sample.to_string(index=False))
            else:
                print("\n⚠️  Database is empty. No records found.")
        else:
            print(f"\n❌ Error: {response.status_code}")
            print(response.text)
    
    except Exception as e:
        print(f"\n❌ Verification error: {e}")
    
    print("\n" + "=" * 70)

# =====================================================
# BATCH UPLOAD (For Large Files)
# =====================================================
def upload_in_batches(excel_file_path, batch_size=500):
    """
    Upload large Excel files in smaller batches
    Useful if you have thousands of rows
    """
    
    print("=" * 70)
    print("BATCH UPLOAD MODE")
    print("=" * 70)
    
    # Read and prepare data (same as above)
    df = pd.read_excel(excel_file_path)
    # ... (apply same cleaning logic as upload_excel_to_database)
    
    total_rows = len(df)
    num_batches = (total_rows // batch_size) + 1
    
    print(f"\n📊 Total rows: {total_rows}")
    print(f"📦 Batch size: {batch_size}")
    print(f"🔢 Number of batches: {num_batches}")
    
    for i in range(0, total_rows, batch_size):
        batch_num = (i // batch_size) + 1
        batch = df.iloc[i:i+batch_size]
        
        print(f"\n📤 Uploading batch {batch_num}/{num_batches} ({len(batch)} records)...")
        
        records = batch.to_dict('records')
        
        try:
            response = requests.post(
                f"{API_BASE_URL}/bulk-upload-other-sources",
                json=records,
                headers=HEADERS
            )
            
            if response.status_code == 200:
                print(f"   ✅ Batch {batch_num} uploaded successfully")
            else:
                print(f"   ❌ Batch {batch_num} failed: {response.text}")
                break
        
        except Exception as e:
            print(f"   ❌ Batch {batch_num} error: {e}")
            break
    
    print("\n" + "=" * 70)

# =====================================================
# MAIN EXECUTION
# =====================================================
if __name__ == "__main__":
    
    # STEP 1: Upload your Excel file
    # Replace with your actual file path
    excel_file = "C:\\Users\\Rowland\\Desktop\\Agriarche\\commodity-model\\data\\live_prices.xlsx"
    
    # Uncomment ONE of these options:
    
    # Option A: Regular upload (recommended for files under 1000 rows)
    upload_excel_to_database(excel_file)
    
    # Option B: Batch upload (for large files with 1000+ rows)
    # upload_in_batches(excel_file, batch_size=500)
    
    # STEP 2: Verify the upload worked
    # verify_uploaded_data()
    
    print("\n⚠️  Please edit this file and:")
    print("   1. Set your Excel file path")
    print("   2. Uncomment the upload function you want to use")
    print("   3. Run the script again")