#!/usr/bin/env python
# coding: utf-8

import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import os
import re
from difflib import get_close_matches
import random

# --------------------------
#  GUI helpers (logging)
# ---------------------------
def create_window():
    root = tk.Tk()
    root.title("📈 Stock Data Merger & Viewer")
    root.geometry("1000x700")
    root.resizable(False, False)
    return root

def configure_log_text(text_widget):
    text_widget.tag_configure("INFO", foreground="black")
    text_widget.tag_configure("SUCCESS", foreground="green")
    text_widget.tag_configure("WARN", foreground="orange")
    text_widget.tag_configure("ERROR", foreground="red")

def log(msg, level="INFO"):
    # insert at end and scroll
    log_box.configure(state="normal")
    log_box.insert(tk.END, msg + "\n", level)
    log_box.see(tk.END)
    log_box.configure(state="disabled")
    root.update_idletasks()

# ---------------------------
#  Core processing functions
# ---------------------------
def safe_read_csv(path):
    try:
        df = pd.read_csv(path)
        df.columns = (
            df.columns
            .str.replace(r"[\n\r\t]+", "", regex=True)
            .str.strip()
        )
        log(f"🧹 Cleaned headers for {os.path.basename(path)}: {list(df.columns)[:7]} ...", "SUCCESS")
        return df
    except Exception as e:
        log(f"⚠️ Error reading {path}: {e}", "ERROR")
        return pd.DataFrame()

def select_cols(df, cols):
    available = [c for c in cols if c in df.columns]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        log(f"⚠️ Missing columns skipped: {missing}", "WARN")
    return df[available].copy() if available else pd.DataFrame()

def smart_normalize_company(df):
    """
    Rename any column that looks like a company column (fuzzy) to 'COMPANY'.
    Works on a copy to avoid SettingWithCopyWarning.
    """
    if df is None or df.empty:
        return df
    df = df.copy()
    for col in list(df.columns):
        # detect anything like 'COMP', 'COMPANY', including typos
        if re.search(r"COMP", col.upper()) and col.upper() != "COMPANY":
            try:
                df.rename(columns={col: "COMPANY"}, inplace=True)
                log(f"   🔄 Renamed '{col}' → 'COMPANY'", "INFO")
            except Exception as e:
                log(f"   ⚠️ Failed renaming {col}: {e}", "WARN")
    # Also handle 'NAME OF COMPANY' explicitly
    if "NAME OF COMPANY" in df.columns and "COMPANY" not in df.columns:
        df.rename(columns={"NAME OF COMPANY": "COMPANY"}, inplace=True)
        log("   🔄 Renamed 'NAME OF COMPANY' → 'COMPANY'", "INFO")
    return df

def normalize_company_col_post_merge(df):
    """Collapse COMPANY_x / COMPANY_y etc. to COMPANY"""
    if df is None or df.empty:
        return df
    df = df.copy()
    for col in list(df.columns):
        if col.startswith("COMPANY_") and "COMPANY" not in df.columns:
            df.rename(columns={col: "COMPANY"}, inplace=True)
            log(f"   🔄 Collapsed '{col}' → 'COMPANY' after merge", "INFO")
        # If multiple company-like columns exist, keep the first and drop duplicates later if needed
    return df

def safe_merge(df1, df2, on=None, left_on=None, right_on=None, how="outer", label=""):
    try:
        log(f"🔗 Merging {label} ...", "INFO")
        merged = pd.merge(df1, df2, on=on, left_on=left_on, right_on=right_on, how=how)
        merged = normalize_company_col_post_merge(merged)
        log(f"✅ Merge complete: rows → {len(merged)}", "SUCCESS")
        return merged
    except Exception as e:
        log(f"⚠️ Merge skipped due to error in {label}: {e}", "ERROR")
        return df1

# ---------------------------
#  The full pipeline (same functionality)
# ---------------------------
def run_pipeline_and_display():
    try:
        # clear previous table and logs
        for r in stock_table.get_children():
            stock_table.delete(r)
        log_box.configure(state="normal")
        log_box.delete(1.0, tk.END)
        log_box.configure(state="disabled")
        root.update_idletasks()

        log("🚀 Starting Data Processing...", "INFO")

        # detect playground folder
        base_dir = os.path.dirname(os.path.abspath(__file__))
        folder_path = os.path.join(base_dir, "playground")
        if not os.path.exists(folder_path):
            log(f"❌ Folder not found: {folder_path}", "ERROR")
            messagebox.showerror("Folder missing", f"playground folder not found at:\n{folder_path}")
            return

        log(f"📁 Searching CSVs inside: {folder_path}", "INFO")

        # list CSVs
        files = [f for f in os.listdir(folder_path) if f.lower().endswith(".csv")]
        if not files:
            log("❌ No CSV files found in playground folder!", "ERROR")
            messagebox.showerror("No CSVs", "No CSV files found in playground folder.")
            return
        log(f"📄 Found CSVs: {files}", "INFO")

        # helper: find file
        def find_file_local(keyword):
            pattern = re.compile(keyword, re.IGNORECASE)
            for f in files:
                if pattern.search(f):
                    log(f"✅ Matched '{keyword}' → {f}", "SUCCESS")
                    return os.path.join(folder_path, f)
            match = get_close_matches(keyword, files, n=1)
            if match:
                log(f"⚠️ Approx match for '{keyword}' → {match[0]}", "WARN")
                return os.path.join(folder_path, match[0])
            log(f"❌ No match found for '{keyword}'", "ERROR")
            return None

        paths = {
            "equity": find_file_local("EQUITY_L"),
            "cf_insider": find_file_local("Insider"),
            "cf_sast_regd": find_file_local("SAST-Regular"),
            "cf_sast_pl": find_file_local("SAST-Pledged"),
            "sec_bhav_data": find_file_local("bhavdata"),
            "cf_shareholding_pattern": find_file_local("Shareholding"),
        }

        missing = [k for k, v in paths.items() if v is None]
        if missing:
            log(f"\n❌ Missing required files for: {', '.join(missing)}", "ERROR")
            messagebox.showerror("Missing files", f"Missing required files: {', '.join(missing)}")
            return

        # load CSVs
        equity = safe_read_csv(paths["equity"])
        cf_insider = safe_read_csv(paths["cf_insider"])
        cf_sast_regd = safe_read_csv(paths["cf_sast_regd"])
        cf_sast_pl = safe_read_csv(paths["cf_sast_pl"])
        sec_bhav_data = safe_read_csv(paths["sec_bhav_data"])
        cf_shareholding_pattern = safe_read_csv(paths["cf_shareholding_pattern"])

        log("\n📊 Selecting required columns (safely)...", "INFO")

        equity_sel = select_cols(equity, ["SYMBOL", "OPEN", "HIGH", "LOW", "PREV. CLOSE"])

        cf_insider_sel = select_cols(cf_insider, [
            'SYMBOL',
            'COMPANY',
            'NAME OF THE ACQUIRER/DISPOSER',
            'VALUE OF SECURITY (ACQUIRED/DISPLOSED)',
            'ACQUISITION/DISPOSAL TRANSACTION TYPE',
        ])

        cf_sast_sel = select_cols(cf_sast_regd, [
            "SYMBOL",
            "COMPANY",
            "TOTAL ACQUISTION (SHARES/VOTING RIGHTS/WARRANTS/ CONVERTIBLE SECURITIES/ ANY OTHER INSTRUMENT)",
        ])

        cf_sast_pl_sel = select_cols(cf_sast_pl, [
            "NAME OF COMPANY",
            "TOTAL PROMOTER HOLDING % A /(A+B+C)",
        ])

        cf_sh_pt_sel = select_cols(cf_shareholding_pattern, ["COMPANY", "PROMOTER & PROMOTER GROUP (A)"])

        bdc_sel = select_cols(sec_bhav_data, ['SYMBOL', 'MARKET', 'SERIES', 'SECURITY', 'PREV_CL_PR'])

        log("✅ Column selection done.", "SUCCESS")
        log("", "INFO")

        # SMART normalization before merges (fuzzy)
        log("🧩 Smart-normalizing company-like columns...", "INFO")
        cf_insider_sel = smart_normalize_company(cf_insider_sel)
        cf_sast_sel = smart_normalize_company(cf_sast_sel)
        cf_sast_pl_sel = smart_normalize_company(cf_sast_pl_sel)
        cf_sh_pt_sel = smart_normalize_company(cf_sh_pt_sel)

        log("🔗 Starting merge process...", "INFO")
        # merges (using safe_merge which also normalizes post-merge)
        equity_cf_insider = safe_merge(equity_sel, cf_insider_sel, on="SYMBOL", how="outer", label="Equity + Insider")
        eq_cf_in_sast = safe_merge(equity_cf_insider, cf_sast_sel, on=["SYMBOL", "COMPANY"], how="outer", label="Add SAST Regular")
        # for pledged file, after fuzzy normalization it should have COMPANY; use on="COMPANY"
        eq_in_sast_pl = safe_merge(eq_cf_in_sast, cf_sast_pl_sel, on="COMPANY", how="outer", label="Add SAST Pledged")
        eq_in_sast_pl_sh = safe_merge(eq_in_sast_pl, cf_sh_pt_sel, on="COMPANY", how="outer", label="Add Shareholding Pattern")
        all_final_data = safe_merge(eq_in_sast_pl_sh, bdc_sel, on="SYMBOL", how="outer", label="Add Bhav Data")

        # Save final file
        output_path = os.path.join(folder_path, "Final_data_auto.csv")
        try:
            all_final_data.to_csv(output_path, index=False)
            log(f"✅ Final merged file saved at: {output_path}", "SUCCESS")
        except Exception as e:
            log(f"❌ Error saving final file: {e}", "ERROR")
            messagebox.showerror("Save error", f"Failed to save final file: {e}")
            return

        # Display first 4 rows in the table
        try:
            if os.path.exists(output_path):
                df_final = pd.read_csv(output_path)
                if df_final.empty:
                    log("⚠️ Final file is empty - nothing to display.", "WARN")
                    messagebox.showwarning("No data", "Final merged file is empty.")
                    return

                # take 6 random companies
                sample_df = df_final.sample(n=min(4, len(df_final)), random_state=random.randint(1, 9999))


                # clear any previous rows
                for r in stock_table.get_children():
                    stock_table.delete(r)

                # If table columns differ, recreate columns (safe)
                # We'll display up to first 8 columns or more if needed
                display_cols = list(sample_df.columns[:8])
                # configure treeview columns
                stock_table["columns"] = display_cols
                for col in display_cols:
                    stock_table.heading(col, text=col)
                    stock_table.column(col, width=110, anchor="w")

                for _, row in sample_df.iterrows():
                    values = [row.get(c, "") for c in display_cols]
                    stock_table.insert("", "end", values=values)

                log("✨ Displayed first 4 rows from the final merged file.", "SUCCESS")
            else:
                log("❌ Final file not found after saving step.", "ERROR")
                messagebox.showerror("Missing file", "Final file not found after save.")
        except Exception as e:
            log(f"❌ Failed to load/display final file: {e}", "ERROR")
            messagebox.showerror("Display error", f"Failed to display final file: {e}")

    except Exception as ex:
        log(f"❌ Unexpected error: {ex}", "ERROR")
        messagebox.showerror("Unexpected error", str(ex))

# ---------------------------
#  Build GUI
# ---------------------------
root = create_window()

# Header
header = tk.Label(root, text="Stock Data Merger & Viewer", font=("Segoe UI", 20, "bold"), fg="#1565C0")
header.pack(pady=10)

# Run Button
run_btn = tk.Button(root, text="▶ Run Data Processing", font=("Segoe UI", 14),
                    bg="#4CAF50", fg="white", command=run_pipeline_and_display,
                    relief="ridge", padx=12, pady=6)
run_btn.pack(pady=8)

# Log Section
log_label = tk.Label(root, text="System Log:", font=("Segoe UI", 12, "bold"))
log_label.pack(anchor="w", padx=18)

log_box = tk.Text(root, height=12, width=118, bg="#F5F5F5", fg="black", wrap="word")
log_box.pack(padx=18, pady=6)
log_box.configure(state="disabled")
configure_log_text(log_box)

# Table for Stock Display
table_label = tk.Label(root, text="📊 First 4 Rows of Final Merged File", font=("Segoe UI", 12, "bold"))
table_label.pack(pady=(12, 4))

frame_table = tk.Frame(root)
frame_table.pack(padx=18, pady=4, fill="both", expand=False)

stock_table = ttk.Treeview(frame_table, show="headings", height=8)
stock_table.pack(side="left", fill="both", expand=True)

scrollbar = ttk.Scrollbar(frame_table, orient="vertical", command=stock_table.yview)
stock_table.configure(yscrollcommand=scrollbar.set)
scrollbar.pack(side="right", fill="y")

# Exit Button
exit_btn = tk.Button(root, text="❌ Exit", command=root.destroy, bg="#E53935", fg="white",
                     font=("Segoe UI", 12), relief="ridge", padx=10, pady=6)
exit_btn.pack(pady=12)

# Start GUI
root.mainloop()
