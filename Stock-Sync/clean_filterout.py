import pandas as pd
import os

# --- Step 1: Define working directory ---
playground_path = r'C:\Users\dhruv\OneDrive\Desktop\ChintanBhai\playground'

# --- Step 2: Check & set working directory ---
if os.path.exists(playground_path):
    os.chdir(playground_path)
    print(f"✅ Working directory set to: {os.getcwd()}")
else:
    raise FileNotFoundError(f"❌ Folder not found: {playground_path}\nPlease verify the path.")

# --- Step 3: Look for final data file ---
possible_files = ['Final_data_auto.csv', 'Final_data.csv']
final_file = next((f for f in possible_files if os.path.exists(f)), None)

if not final_file:
    raise FileNotFoundError(f"❌ None of the final data files found in {os.getcwd()}.\nExpected one of: {possible_files}")

print(f"📄 Found final data file: {final_file}")

# --- Step 4: Load data safely ---
final_data = pd.read_csv(final_file)
print(f"✅ Loaded data with {len(final_data)} rows and {len(final_data.columns)} columns")

# --- Step 5: Define the important columns to keep ---
columns_to_keep = [
    'SYMBOL',
    'FACE VALUE',
    'COMPANY_x',
    'VALUE OF SECURITY (ACQUIRED/DISPLOSED)',
    'ACQUISITION/DISPOSAL TRANSACTION TYPE',
    'TOTAL AFTER ACQUISITION/SALE (SHARES/VOTING RIGHTS/WARRANTS/ CONVERTIBLE SECURITIES/ANY OTHER INSTRUMENT)',
    'CLOSE_PRICE',
    'AVG_PRICE',
    'DELIV_PER'
]

# Keep only columns that exist
existing_cols = [col for col in columns_to_keep if col in final_data.columns]
if not existing_cols:
    raise ValueError("❌ None of the expected columns found. Please verify the input file structure.")

print(f"🧹 Keeping columns: {existing_cols}")

# --- Step 6: Clean & rename for clarity ---
cleaned_data = final_data[existing_cols].copy()
rename_map = {
    'SYMBOL': 'Symbol',
    'FACE VALUE': 'Face Value',
    'COMPANY_x': 'Company',
    'VALUE OF SECURITY (ACQUIRED/DISPLOSED)': 'Value of Security',
    'ACQUISITION/DISPOSAL TRANSACTION TYPE': 'Transaction Type',
    'TOTAL AFTER ACQUISITION/SALE (SHARES/VOTING RIGHTS/WARRANTS/ CONVERTIBLE SECURITIES/ANY OTHER INSTRUMENT)': 'Total After Acquisition',
    'CLOSE_PRICE': 'Close Price',
    'AVG_PRICE': 'Average Price',
    'DELIV_PER': 'Delivery Percentage'
}
cleaned_data.rename(columns=rename_map, inplace=True)

# --- Step 7: Sort and handle missing data ---
cleaned_data.sort_values(by='Symbol', inplace=True, na_position='last')
cleaned_data.fillna('', inplace=True)

# --- Step 8: Save outputs ---
excel_output = "Cleaned_Final_Data.xlsx"
csv_output = "Cleaned_Final_Data.csv"

cleaned_data.to_excel(excel_output, index=False)
cleaned_data.to_csv(csv_output, index=False)

print(f"✅ Cleaned data saved as:\n   - {excel_output}\n   - {csv_output}")
print(f"📊 Total rows: {len(cleaned_data)}, Columns: {len(cleaned_data.columns)}")
print("🎉 Cleaning and export completed successfully!")
