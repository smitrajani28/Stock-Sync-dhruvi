# Stock_Data_Merge.py
import pandas as pd
import os
import re
from difflib import get_close_matches

def safe_read_csv(path):
    try:
        df = pd.read_csv(path)
        df.columns = df.columns.str.replace(r"[\n\r\t]+", "", regex=True).str.strip()
        return df
    except:
        return pd.DataFrame()

def select_cols(df, cols):
    available = [c for c in cols if c in df.columns]
    return df[available].copy() if available else pd.DataFrame()

def smart_normalize_company(df):
    if df is None or df.empty:
        return df
    df = df.copy()
    for col in list(df.columns):
        if re.search(r"COMP", col.upper()) and col.upper() != "COMPANY":
            df.rename(columns={col: "COMPANY"}, inplace=True)
    if "NAME OF COMPANY" in df.columns and "COMPANY" not in df.columns:
        df.rename(columns={"NAME OF COMPANY": "COMPANY"}, inplace=True)
    return df

def run_merge():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(base_dir, "playground")

    files = [f for f in os.listdir(folder_path) if f.lower().endswith(".csv")]

    def find(keyword):
        for f in files:
            if re.search(keyword, f, re.IGNORECASE):
                return os.path.join(folder_path, f)
        match = get_close_matches(keyword, files, n=1)
        return os.path.join(folder_path, match[0]) if match else None

    paths = {
        "equity": find("EQUITY_L"),
        "cf_insider": find("Insider"),
        "cf_sast_regd": find("SAST-Regular"),
        "cf_sast_pl": find("SAST-Pledged"),
        "sec_bhav_data": find("bhavdata"),
        "cf_shareholding_pattern": find("Shareholding"),
    }

    # Load
    equity = safe_read_csv(paths["equity"])
    cf_insider = safe_read_csv(paths["cf_insider"])
    cf_sast = safe_read_csv(paths["cf_sast_regd"])
    cf_sast_pl = safe_read_csv(paths["cf_sast_pl"])
    bhav = safe_read_csv(paths["sec_bhav_data"])
    sharehold = safe_read_csv(paths["cf_shareholding_pattern"])

    # Select cols
    equity = select_cols(equity, ["SYMBOL", "OPEN", "HIGH", "LOW", "PREV. CLOSE"])
    cf_insider = smart_normalize_company(select_cols(cf_insider, ["SYMBOL", "COMPANY"]))
    cf_sast = smart_normalize_company(select_cols(cf_sast, ["SYMBOL", "COMPANY"]))
    cf_sast_pl = smart_normalize_company(select_cols(cf_sast_pl, ["COMPANY"]))
    sharehold = smart_normalize_company(select_cols(sharehold, ["COMPANY"]))
    bhav = select_cols(bhav, ["SYMBOL", "MARKET", "SERIES", "SECURITY", "PREV_CL_PR"])

    # Merge
    df = equity.merge(cf_insider, on="SYMBOL", how="outer")
    df = df.merge(cf_sast, on=["SYMBOL", "COMPANY"], how="outer")
    df = df.merge(cf_sast_pl, on="COMPANY", how="outer")
    df = df.merge(sharehold, on="COMPANY", how="outer")
    df = df.merge(bhav, on="SYMBOL", how="outer")

    # Save
    output_path = os.path.join(folder_path, "Cleaned_Final_Data.xlsx")
    df.to_excel(output_path, index=False)

    return output_path
