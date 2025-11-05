#!/usr/bin/env python
# coding: utf-8

import streamlit as st
import pandas as pd
import os
import subprocess
import io

# --- Configuration ---
BASE_DIR = os.getcwd()
PLAYGROUND_DIR = os.path.join(BASE_DIR, "playground")
FINAL_FILE = os.path.join(PLAYGROUND_DIR, "Cleaned_Final_Data.xlsx")

# --- Streamlit UI Setup ---
st.set_page_config(page_title="📊 Stock Data Merger Tool", layout="wide")

st.title("📈 Stock Data Merger & Viewer (Web Version)")
st.caption("Run your full data processing pipeline directly from your browser!")

# --- Run Button ---
if st.button("▶ Run Full Data Merge"):
    with st.spinner("Running Stock_Data_App.py..."):
        try:
            subprocess.run(["python", "Stock_Data_App.py"], check=True)
            st.success("✅ Stock_Data_App.py completed successfully!")
        except subprocess.CalledProcessError as e:
            st.error(f"❌ Error running Stock_Data_App.py: {e}")
        except Exception as e:
            st.error(f"⚠️ Unexpected error: {e}")

# --- Display Section ---
st.subheader("📊 Latest Processed Stock Data")

if os.path.exists(FINAL_FILE):
    try:
        df = pd.read_excel(FINAL_FILE)
        if not df.empty:
            st.success(f"✅ Loaded {len(df)} records from {os.path.basename(FINAL_FILE)}")

            # --- Show 6 random samples ---
            # --- Show Full Data ---
            st.dataframe(df.head(), use_container_width=True)

            # --- Prepare Excel file for download ---
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False)
            buffer.seek(0)

            # --- Download Button ---
            st.download_button(
                label="⬇️ Download Final Excel",
                data=buffer,
                file_name="Cleaned_Final_Data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("⚠️ The cleaned data file is empty. Please rerun the process.")
    except Exception as e:
        st.error(f"❌ Failed to read Excel file: {e}")
else:
    st.info("ℹ️ No merged data found yet. Click the button above to generate it.")

st.markdown("---")
st.caption("Made with ❤️ by Dhruvi | Stock Data Processing Suite")
