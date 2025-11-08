# Stock_Data_App.py
import os
import re
import pandas as pd

def _ensure_company_col(df):
    # if already present — OK
    if "COMPANY" in df.columns:
        return df
    # try fuzzy detections
    for col in df.columns:
        if re.search(r"COMP", col, re.IGNORECASE) or re.search(r"NAME OF COMPANY", col, re.IGNORECASE):
            try:
                df = df.rename(columns={col: "COMPANY"})
                return df
            except Exception:
                pass
    # fallback: create empty COMPANY col so downstream doesn't crash
    df["COMPANY"] = ""
    return df

def run_pipeline(folder_path=None):
    """
    Run full pipeline and return final dataframe and output_path.
    folder_path: path to playground folder. If None, uses script dir/playground
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if folder_path is None:
        folder_path = os.path.join(base_dir, "playground")

    # (1) locate your CSV files — same logic you had before
    files = [f for f in os.listdir(folder_path) if f.lower().endswith(".csv")]

    def find_file_local(keyword):
        pattern = re.compile(keyword, re.IGNORECASE)
        for f in files:
            if pattern.search(f):
                return os.path.join(folder_path, f)
        return None

    paths = {
        "equity": find_file_local("EQUITY_L"),
        "cf_insider": find_file_local("Insider"),
        "cf_sast_regd": find_file_local("SAST-Regular"),
        "cf_sast_pl": find_file_local("SAST-Pledged"),
        "sec_bhav_data": find_file_local("bhavdata"),
        "cf_shareholding_pattern": find_file_local("Shareholding"),
    }

    # raise if any missing
    missing = [k for k,v in paths.items() if v is None]
    if missing:
        raise FileNotFoundError(f"Missing required files: {missing}")

    # safe read helper
    def safe_read_csv(p):
        df = pd.read_csv(p)
        df.columns = df.columns.str.replace(r"[\n\r\t]+", "", regex=True).str.strip()
        return df

    equity = safe_read_csv(paths["equity"])
    cf_insider = safe_read_csv(paths["cf_insider"])
    cf_sast_regd = safe_read_csv(paths["cf_sast_regd"])
    cf_sast_pl = safe_read_csv(paths["cf_sast_pl"])
    sec_bhav_data = safe_read_csv(paths["sec_bhav_data"])
    cf_shareholding_pattern = safe_read_csv(paths["cf_shareholding_pattern"])

    # Ensure COMPANY exists in each relevant DF before merging
    cf_insider = _ensure_company_col(cf_insider)
    cf_sast_regd = _ensure_company_col(cf_sast_regd)
    cf_sast_pl = _ensure_company_col(cf_sast_pl)
    cf_shareholding_pattern = _ensure_company_col(cf_shareholding_pattern)
    equity = _ensure_company_col(equity)
    sec_bhav_data = _ensure_company_col(sec_bhav_data)

    # Now select the columns you need (safe selection)
    def sel(df, cols):
        available = [c for c in cols if c in df.columns]
        return df[available].copy() if available else pd.DataFrame()

    equity_sel = sel(equity, ["SYMBOL", "OPEN", "HIGH", "LOW", "PREV. CLOSE"])
    cf_insider_sel = sel(cf_insider, ['SYMBOL','COMPANY','NAME OF THE ACQUIRER/DISPOSER','VALUE OF SECURITY (ACQUIRED/DISPLOSED)','ACQUISITION/DISPOSAL TRANSACTION TYPE'])
    cf_sast_sel = sel(cf_sast_regd, ["SYMBOL","COMPANY","TOTAL ACQUISTION (SHARES/VOTING RIGHTS/WARRANTS/ CONVERTIBLE SECURITIES/ ANY OTHER INSTRUMENT)"])
    cf_sast_pl_sel = sel(cf_sast_pl, ["NAME OF COMPANY","TOTAL PROMOTER HOLDING % A /(A+B+C)"])
    cf_sh_pt_sel = sel(cf_shareholding_pattern, ["COMPANY","PROMOTER & PROMOTER GROUP (A)"])
    bdc_sel = sel(sec_bhav_data, ['SYMBOL','MARKET','SERIES','SECURITY','PREV_CL_PR'])

    # Rename 'NAME OF COMPANY' -> COMPANY if exists
    if "NAME OF COMPANY" in cf_sast_pl_sel.columns and "COMPANY" not in cf_sast_pl_sel.columns:
        cf_sast_pl_sel = cf_sast_pl_sel.rename(columns={"NAME OF COMPANY":"COMPANY"})

    # Do merges (example)
    df = equity_sel
    df = pd.merge(df, cf_insider_sel, on="SYMBOL", how="outer")
    # try merge on SYMBOL+COMPANY if both present
    try:
        df = pd.merge(df, cf_sast_sel, on=["SYMBOL","COMPANY"], how="outer")
    except Exception:
        df = pd.merge(df, cf_sast_sel, on="SYMBOL", how="outer")

    try:
        df = pd.merge(df, cf_sast_pl_sel, on="COMPANY", how="outer")
    except Exception:
        pass
    try:
        df = pd.merge(df, cf_sh_pt_sel, on="COMPANY", how="outer")
    except Exception:
        pass
    df = pd.merge(df, bdc_sel, on="SYMBOL", how="outer")

    # Ensure COMPANY column is present
    df = _ensure_company_col(df)

    # Save outputs
    output_csv = os.path.join(folder_path, "Final_data_auto.csv")
    df.to_csv(output_csv, index=False)
    output_xlsx = os.path.join(folder_path, "Cleaned_Final_Data.xlsx")
    try:
        df.to_excel(output_xlsx, index=False)
    except Exception:
        pass

    return df, output_xlsx
