import sqlite3
import pandas as pd
import os

# This finds ANY excel file in your current folder
files = [f for f in os.listdir('.') if f.endswith('.xlsx')]

if not files:
    print("❌ Error: No Excel file (.xlsx) found in this folder!")
else:
    # It picks the first Excel file it finds (the one you showed in 'dir')
    excel_file = files[0]
    try:
        print(f"🔄 Found: {excel_file}. Starting migration...")
        df = pd.read_excel(excel_file)
        
        # Clean columns to lowercase so 'commodity' error stops
        df.columns = [str(col).lower().strip() for col in df.columns]
        
        # Create the Database
        conn = sqlite3.connect('kasuwa.db')
        df.to_sql('prices', conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"✅ Success! 'kasuwa.db' created using {excel_file}")
    except Exception as e:
        print(f"❌ Error: {e}")