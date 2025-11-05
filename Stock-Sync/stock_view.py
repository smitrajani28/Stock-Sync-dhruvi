#!/usr/bin/env python
# coding: utf-8

import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import os

# ===============================
# 🧩 STEP 1: Load Cleaned Data
# ===============================
def load_data():
    try:
        # Look for file in current directory
        if not os.path.exists("Cleaned_Final_Data.xlsx"):
            messagebox.showerror("Error", "❌ 'Cleaned_Final_Data.xlsx' not found!\n\nPlease run the data scripts first.")
            return pd.DataFrame()

        df = pd.read_excel("Cleaned_Final_Data.xlsx")
        if df.empty:
            messagebox.showinfo("Info", "⚠️ The file is empty. Please check your Excel file.")
            return pd.DataFrame()

        return df
    except Exception as e:
        messagebox.showerror("Error", f"Error loading data:\n{e}")
        return pd.DataFrame()

# ===============================
# 🧩 STEP 2: Show First 4 Stocks
# ===============================
def show_first_4():
    df = load_data()
    if df.empty:
        return

    # Take first 4 rows only
    sample_df = df.head(4)

    # Clear old data
    for row in tree.get_children():
        tree.delete(row)

    # Insert rows into table
    for _, row in sample_df.iterrows():
        tree.insert("", tk.END, values=list(row))

    status_label.config(text=f"✅ Showing first {len(sample_df)} stocks from Cleaned_Final_Data.xlsx")

# ===============================
# 🪟 STEP 3: GUI Layout
# ===============================
root = tk.Tk()
root.title("📈 Simple Stock Dashboard")
root.geometry("950x420")
root.configure(bg="#f4f6f8")

# Title
title_label = tk.Label(root, text="📊 Stock Summary Dashboard", font=("Segoe UI", 18, "bold"), bg="#f4f6f8", fg="#2c3e50")
title_label.pack(pady=15)

# Columns (as per your cleaned Excel)
columns = ["Symbol", "Face Value", "Company", "Value of Security", "Transaction Type", "Total After Acquisition"]

tree = ttk.Treeview(root, columns=columns, show="headings", height=6)
style = ttk.Style()
style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))
style.configure("Treeview", font=("Segoe UI", 10))

for col in columns:
    tree.heading(col, text=col)
    tree.column(col, width=150, anchor="center")

tree.pack(pady=10, fill=tk.X, padx=30)

# Buttons
button_frame = tk.Frame(root, bg="#f4f6f8")
button_frame.pack(pady=10)

refresh_btn = tk.Button(button_frame, text="🔄 Refresh Data", command=show_first_4, font=("Segoe UI", 11, "bold"),
                        bg="#007bff", fg="white", width=15)
refresh_btn.grid(row=0, column=0, padx=10)

open_excel_btn = tk.Button(button_frame, text="📂 Open Excel File", font=("Segoe UI", 11, "bold"),
                           bg="#28a745", fg="white", width=15,
                           command=lambda: os.startfile("Cleaned_Final_Data.xlsx") if os.path.exists("Cleaned_Final_Data.xlsx") else messagebox.showerror("Error", "Excel file not found!"))
open_excel_btn.grid(row=0, column=1, padx=10)

exit_btn = tk.Button(button_frame, text="❌ Exit", command=root.destroy, font=("Segoe UI", 11, "bold"),
                     bg="#dc3545", fg="white", width=15)
exit_btn.grid(row=0, column=2, padx=10)

# Status bar
status_label = tk.Label(root, text="ℹ️ Ready", font=("Segoe UI", 9), bg="#f4f6f8", fg="#555")
status_label.pack(side=tk.BOTTOM, pady=5)

# Load on start
show_first_4()

root.mainloop()
