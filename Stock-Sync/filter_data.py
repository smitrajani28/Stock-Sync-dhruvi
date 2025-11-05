import pandas as pd
import os

# --- Step 1: Define your correct working directory ---
playground_path = r'C:\Users\dhruv\OneDrive\Desktop\ChintanBhai\playground'

# --- Step 2: Verify and set working directory safely ---
if os.path.exists(playground_path):
    os.chdir(playground_path)
    print(f"✅ Working directory set to: {os.getcwd()}")
else:
    raise FileNotFoundError(f"❌ Folder not found: {playground_path}\nPlease check the path or update it in the script.")

# --- Step 3: Verify available files ---
print("📁 Files found in playground:")
print(os.listdir())

# --- Step 4: Load your final data ---
final_file = 'Final_data_auto.csv'  # or Final_data.csv depending on your pipeline
if not os.path.exists(final_file):
    raise FileNotFoundError(f"❌ The file '{final_file}' was not found in {os.getcwd()}")

final_data = pd.read_csv(final_file)

# --- Step 5: Clean and select specific columns ---
columns_to_keep = [
    'SYMBOL',
    'FACE VALUE',
    'COMPANY_x',
    'VALUE OF SECURITY (ACQUIRED/DISPLOSED)',
    'ACQUISITION/DISPOSAL TRANSACTION TYPE',
    'TOTAL AFTER ACQUISITION/SALE (SHARES/VOTING RIGHTS/WARRANTS/ CONVERTIBLE SECURITIES/ANY OTHER INSTRUMENT)'
]

# Filter only existing columns (avoid KeyError)
available_columns = [col for col in columns_to_keep if col in final_data.columns]
cleaned_data = final_data[available_columns].copy()

# --- Step 6: Rename for clarity ---
rename_map = {
    'SYMBOL': 'Symbol',
    'FACE VALUE': 'Face Value',
    'COMPANY_x': 'Company',
    'VALUE OF SECURITY (ACQUIRED/DISPLOSED)': 'Value of Security',
    'ACQUISITION/DISPOSAL TRANSACTION TYPE': 'Transaction Type',
    'TOTAL AFTER ACQUISITION/SALE (SHARES/VOTING RIGHTS/WARRANTS/ CONVERTIBLE SECURITIES/ANY OTHER INSTRUMENT)': 'Total After Acquisition'
}

cleaned_data.rename(columns=rename_map, inplace=True)

# --- Step 7: Sort and export ---
cleaned_data = cleaned_data.sort_values(by='Symbol', na_position='last')
output_file = "Cleaned_Final_Data.xlsx"
cleaned_data.to_excel(output_file, index=False)

print(f"✅ Cleaned data saved as: {os.path.join(os.getcwd(), output_file)}")
