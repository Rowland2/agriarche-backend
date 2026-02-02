import sqlite3
import pandas as pd

# 1. Load the Excel file
# Change this line in backend/db_setup.py
excel_file = "predictive Analysis Commodity Pricing" # Match the exact name in your sidebar

try:
    df = pd.read_excel(excel_file)
    # 2. Clean the column names right now so we avoid KeyErrors later
    df.columns = [str(col).lower().strip() for col in df.columns]

    # 3. Create the Database and save the data
    conn = sqlite3.connect('kasuwa.db')
    df.to_sql('prices', conn, if_exists='replace', index=False)
    conn.close()

    print("✅ Success! 'kasuwa.db' created with cleaned column names.")
except Exception as e:
    print(f"❌ Error: {e}")