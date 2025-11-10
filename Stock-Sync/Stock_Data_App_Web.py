# Stock_Data_App_Web.py
import streamlit as st
import io
import pandas as pd
from Stock_Data_App import run_pipeline
import os

import subprocess
import sys

# Path to requirements.txt (auto-detect if in same folder)
req_file = os.path.join(os.path.dirname(__file__), "requirements.txt")

if os.path.exists(req_file):
    try:
        # Try installing required packages
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", req_file])
        print("✅ All required packages installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Failed to install requirements: {e}")
else:
    print("⚠️ requirements.txt not found, skipping package installation.")





st.set_page_config(page_title="📊 Stock Data Merger Tool", layout="wide")
st.title("📈 Stock Data Merger & Viewer (Web Version)")
st.caption("Run your full data processing pipeline directly from your browser!")

# Define path to existing processed file ONCE here ✅
base_dir = os.path.dirname(os.path.abspath(__file__))
play = os.path.join(base_dir, "Stock-Sync/playground", "Cleaned_Final_Data.xlsx")

# placeholders
status = st.empty()
df_area = st.empty()

# ▶ Run Pipeline Button
if st.button("▶ Run Full Data Merge"):
    try:
        status.info("Running pipeline...")
        df, output_xlsx = run_pipeline()

        if df is None or df.empty:
            status.warning("Pipeline finished but returned no data.")
        else:
            # COMPANY First Column
            cols = list(df.columns)
            if "COMPANY" in cols:
                df = df[["COMPANY"] + [c for c in cols if c != "COMPANY"]]

            status.success(f"✅ Pipeline completed — {len(df)} rows.")

            # Show 4 random rows
            sample_df = df.sample(4) if len(df) >= 4 else df
            df_area.dataframe(sample_df)

            # Download Button
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False)
            buffer.seek(0)

            st.download_button(
                label="⬇️ Download Final Excel",
                data=buffer,
                file_name=os.path.basename(output_xlsx) if output_xlsx else "Cleaned_Final_Data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    except Exception as e:
        status.error(f"❌ Error: {e}")

# If not running pipeline, load existing file
else:
    if os.path.exists(play):
        try:
            df0 = pd.read_excel(play)

            # COMPANY first
            if "COMPANY" in df0.columns:
                df0 = df0[["COMPANY"] + [c for c in df0.columns if c != "COMPANY"]]

            # Show 4 random rows
            sample_df = df0.sample(4) if len(df0) >= 4 else df0
            df_area.dataframe(sample_df)

            status.info(f"Loaded existing file: {os.path.basename(play)} ({len(df0)} rows)")
        except Exception as e:
            status.warning(f"⚠️ Could not load existing file: {e}")
    else:
        status.info("No processed file found — press Run to create it.")
