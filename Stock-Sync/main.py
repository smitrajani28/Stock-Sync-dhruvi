#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import re
from difflib import get_close_matches

# 🗂️ Automatically detect playground folder in the current directory
base_dir = os.path.dirname(os.path.abspath(__file__))
folder_path = os.path.join(base_dir, "playground")

if not os.path.exists(folder_path):
    print(f"❌ Folder not found: {folder_path}")
    print("👉 Make sure 'playground' folder is in the same directory as this script.")
    exit()

print(f"📁 Searching CSVs inside: {folder_path}\n")

# 🔍 List all CSV files in folder
files = [f for f in os.listdir(folder_path) if f.lower().endswith(".csv")]
if not files:
    print("❌ No CSV files found in playground folder!")
    exit()

print("📄 Found CSVs:", files, "\n")

# 🔎 Helper: Find best match file
def find_file(keyword):
    pattern = re.compile(keyword, re.IGNORECASE)
    for f in files:
        if pattern.search(f):
            print(f"✅ Matched '{keyword}' → {f}")
            return os.path.join(folder_path, f)
    match = get_close_matches(keyword, files, n=1)
    if match:
        print(f"⚠️ Approx match for '{keyword}' → {match[0]}")
        return os.path.join(folder_path, match[0])
    print(f"❌ No match found for '{keyword}'")
    return None

# 🔗 Find all required CSVs
paths = {
    "equity": find_file("EQUITY_L"),
    "cf_insider": find_file("Insider"),
    "cf_sast_regd": find_file("SAST-Regular"),
    "cf_sast_pl": find_file("SAST-Pledged"),
    "sec_bhav_data": find_file("bhavdata"),
    "cf_shareholding_pattern": find_file("Shareholding"),
}

# 🚨 Check missing files
missing = [k for k, v in paths.items() if v is None]
if missing:
    print(f"\n❌ Missing required files for: {', '.join(missing)}")
    exit()

print("\n📥 Loading all matched CSVs...\n")

# 🧾 Load CSVs safely with header cleanup
def safe_read_csv(path):
    try:
        df = pd.read_csv(path)
        # 🧼 Clean headers: remove spaces, newlines, tabs
        df.columns = (
            df.columns
            .str.replace(r"[\n\r\t]+", "", regex=True)
            .str.strip()
        )
        print(f"🧹 Cleaned headers for {os.path.basename(path)}: {list(df.columns)[:5]} ...")
        return df
    except Exception as e:
        print(f"⚠️ Error reading {path}: {e}")
        return pd.DataFrame()


equity = safe_read_csv(paths["equity"])
cf_insider = safe_read_csv(paths["cf_insider"])
cf_sast_regd = safe_read_csv(paths["cf_sast_regd"])
cf_sast_pl = safe_read_csv(paths["cf_sast_pl"])
sec_bhav_data = safe_read_csv(paths["sec_bhav_data"])
cf_shareholding_pattern = safe_read_csv(paths["cf_shareholding_pattern"])

print("✅ All CSVs loaded successfully!\n")

# 🧩 Select columns safely
def select_cols(df, cols):
    available = [c for c in cols if c in df.columns]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"⚠️ Missing columns skipped: {missing}")
    return df[available]
equity_sel = select_cols(equity, ["SYMBOL", "OPEN", "HIGH", "LOW", "PREV. CLOSE"])

cf_insider_sel = select_cols(cf_insider, [
    'SYMBOL',
    'COMPANY',
    'NAME OF THE ACQUIRER/DISPOSER',
    'VALUE OF SECURITY (ACQUIRED/DISPLOSED)',
    'ACQUISITION/DISPOSAL TRANSACTION TYPE',
    '% SHAREHOLDING (PRIOR)',
    '% POST'
])

cf_sast_sel = select_cols(cf_sast_regd, [
    "SYMBOL",
    "COMPANY",
    "TOTAL AFTER ACQUISITION/SALE (SHARES/VOTING RIGHTS/WARRANTS/ CONVERTIBLE SECURITIES/ANY OTHER INSTRUMENT)",
])
cf_sast_pl_sel = select_cols(cf_sast_pl, [
    "NAME OF COMPANY",
    "PROMOTER SHARES ENCUMBERED AS OF LAST QUARTER % OF TOTAL SHARES [X/(A+B+C)]",
])
cf_sh_pt_sel = select_cols(cf_shareholding_pattern, ["COMPANY", "PROMOTER & PROMOTER GROUP (A)"])
bdc_sel = select_cols(sec_bhav_data, ['SYMBOL', 'CLOSE_PRICE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'NET_TRDQTY'])

print("\n📊 Columns selected successfully!\n")

# 🔄 Safe merge
# 🔄 Safe merge
def safe_merge(df1, df2, on=None, left_on=None, right_on=None, how="outer", label=""):
    try:
        print(f"🔗 Merging {label} ...")
        # Check that keys exist
        keys = [on] if on else [left_on, right_on]
        keys = [k for k in keys if k is not None]
        for k in keys:
            if isinstance(k, list):
                for subk in k:
                    if subk and subk not in df1.columns and subk not in df2.columns:
                        print(f"⚠️ Key '{subk}' not found in one of the DataFrames.")
                        return df1
            elif k and k not in df1.columns and k not in df2.columns:
                print(f"⚠️ Key '{k}' not found in one of the DataFrames.")
                return df1

        merged = pd.merge(df1, df2, on=on, left_on=left_on, right_on=right_on, how=how)
        print(f"✅ Merge complete: rows → {len(merged)}")
        return merged
    except Exception as e:
        print(f"⚠️ Merge skipped due to error: {e}")
        return df1
    
    # 🧭 Normalize company name columns before merging
for df in [equity_sel, cf_insider_sel, cf_sast_sel, cf_sast_pl_sel, cf_sh_pt_sel, bdc_sel]:
    if "NAME OF COMPANY" in df.columns and "COMPANY" not in df.columns:
        df.rename(columns={"NAME OF COMPANY": "COMPANY"}, inplace=True)


# 🔗 Step-by-step merges
equity_cf_insider = safe_merge(equity_sel, cf_insider_sel, on="SYMBOL", how="outer", label="Equity + Insider")
eq_cf_in_sast = safe_merge(equity_cf_insider, cf_sast_sel, on=["SYMBOL", "COMPANY"], how="outer", label="Add SAST Regular")
eq_in_sast_pl = safe_merge(eq_cf_in_sast, cf_sast_pl_sel,
                           left_on="COMPANY", right_on="NAME OF COMPANY",
                           how="outer", label="Add SAST Pledged").drop(columns=["NAME OF COMPANY"], errors="ignore")
eq_in_sast_pl_sh = safe_merge(eq_in_sast_pl, cf_sh_pt_sel, on="COMPANY", how="outer", label="Add Shareholding Pattern")
all_final_data = safe_merge(eq_in_sast_pl_sh, bdc_sel, on="SYMBOL", how="outer", label="Add Bhav Data")

print("\n🔗 All data merged successfully!\n")

# 💾 Save final file
output_path = os.path.join(folder_path, "Final_data_auto.csv")
try:
    all_final_data.to_csv(output_path, index=False)
    print(f"✅ Final merged file saved at:\n{output_path}")
except Exception as e:
    print(f"❌ Error saving final file: {e}")
