"""
Using machine learning ( Isolation forest ) to detect anamoly.
Final script extracted from notebooks
"""

import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# Ensure project root is on sys.path so `from db import load_engine` works when running script directly
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import load_engine

# CONFIG
OUTPUT_DIR  = os.path.join(os.path.dirname(__file__), "..", "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

engine = load_engine()
print("Connected to Adventureworks")

# PULL RAW PO LINE ITEMS 
print("Loading PO line items")

sql = """
SELECT
    poh.purchaseorderid,
    pod.purchaseorderdetailid,
    poh.vendorid,
    v.name                                              AS vendor_name,
    v.creditrating,
    pod.productid,
    p.name                                              AS product_name,
    pc.name                                             AS category,
    pod.orderqty,
    pod.unitprice,
    pod.receivedqty,
    pod.rejectedqty,
    p.standardcost,
    ROUND(pod.orderqty * pod.unitprice, 2)              AS line_total,
    poh.subtotal                                        AS po_subtotal,
    poh.orderdate,
    EXTRACT(YEAR  FROM poh.orderdate)                   AS order_year,
    EXTRACT(MONTH FROM poh.orderdate)                   AS order_month,
    EXTRACT(DOW   FROM poh.orderdate)                   AS order_dow,
    poh.status                                          AS po_status
FROM purchasing.purchaseorderheader poh
JOIN purchasing.purchaseorderdetail pod
    ON poh.purchaseorderid = pod.purchaseorderid
JOIN purchasing.vendor v
    ON poh.vendorid = v.businessentityid
JOIN production.product p
    ON pod.productid = p.productid
LEFT JOIN production.productsubcategory psc
    ON p.productsubcategoryid = psc.productsubcategoryid
LEFT JOIN production.productcategory pc
    ON psc.productcategoryid = pc.productcategoryid
"""

df = pd.read_sql(sql, engine)
print(f"  Loaded {len(df):,} PO line items\n")

#FEATURE ENGINEERING 
print("Feature Engineering")

# Per vendor-product: compute avg price and std dev
df["vendor_product_key"] = df["vendorid"].astype(str) + "_" + df["productid"].astype(str)

vp_stats = (df.groupby("vendor_product_key")["unitprice"]
              .agg(vp_avg_price="mean", vp_std_price="std", vp_count="count")
              .reset_index())

df = df.merge(vp_stats, on="vendor_product_key", how="left")

# Price deviation from vendor-product average
df["price_deviation"]   = df["unitprice"] - df["vp_avg_price"]
df["price_deviation_pct"] = df["price_deviation"] / df["vp_avg_price"].replace(0, np.nan) * 100

# Z-score of unit price within vendor-product group
df["z_score"] = df["price_deviation"] / df["vp_std_price"].replace(0, np.nan)
df["z_score"] = df["z_score"].fillna(0)

# Price vs standard cost ratio
df["price_to_stdcost_ratio"] = df["unitprice"] / df["standardcost"].replace(0, np.nan)
df["price_to_stdcost_ratio"] = df["price_to_stdcost_ratio"].fillna(1)

# Rejection rate for this line
df["rejection_rate"] = df["rejectedqty"] / df["orderqty"].replace(0, np.nan)
df["rejection_rate"] = df["rejection_rate"].fillna(0)

# Line total as % of PO subtotal
df["line_pct_of_po"] = df["line_total"] / df["po_subtotal"].replace(0, np.nan) * 100
df["line_pct_of_po"] = df["line_pct_of_po"].fillna(0)

print(f"  Features engineered: {df.shape[1]} columns")
print(f"  Price deviation range: {df['price_deviation'].min():.2f} → {df['price_deviation'].max():.2f}")
print(f"  Z-score range: {df['z_score'].min():.2f} → {df['z_score'].max():.2f}\n")



# Model Training
print("Started to train isolation forest")

features = [
    "unitprice",
    "orderqty",
    "line_total",
    "z_score",
    "price_to_stdcost_ratio",
    "rejection_rate",
    "line_pct_of_po",
    "price_deviation_pct",
]

X = df[features].fillna(0)

# Standardization
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# contamination = expected proportion of anomalies (~3%)
iso = IsolationForest(
    n_estimators=200,
    contamination=0.03,
    random_state=42,
    n_jobs=-1
)
df["anomaly_score"]  = iso.fit_predict(X_scaled)      # -1 = anomaly, 1 = normal
df["anomaly_raw"]    = iso.score_samples(X_scaled)    # lower = more anomalous
df["is_anomaly"]     = df["anomaly_score"] == -1

n_anomalies = df["is_anomaly"].sum()
print(f"Anomalies detected: {n_anomalies} of {len(df):,} "
      f"({n_anomalies/len(df)*100:.1f}%)")

# Human-readable flag
df["anomaly_flag"] = df.apply(
    lambda r: (
        "High Price Spike"       if r["is_anomaly"] and r["z_score"] > 2  else
        "Unusually Low Price"    if r["is_anomaly"] and r["z_score"] < -2 else
        "High Rejection Rate"    if r["is_anomaly"] and r["rejection_rate"] > 0.2 else
        "Abnormal Order Volume"  if r["is_anomaly"] else
        "Normal"
    ), axis=1
)

print(f"Anomaly breakdown:")
print(df[df["is_anomaly"]]["anomaly_flag"].value_counts().to_string())

# Export result
print("Exporting results ")

# Full anomaly report
report_cols = [
    "purchaseorderid", "purchaseorderdetailid", "vendor_name",
    "product_name", "category", "orderqty", "unitprice",
    "standardcost", "vp_avg_price", "price_deviation",
    "price_deviation_pct", "z_score", "rejection_rate",
    "line_total", "anomaly_raw", "is_anomaly", "anomaly_flag",
    "orderdate", "order_year", "order_month",
]
anomaly_df = df[report_cols].sort_values("anomaly_raw")
anomaly_df.to_csv(os.path.join(OUTPUT_DIR, "anomaly_report.csv"), index=False)
print(f"File saved to: outputs/anomaly_report.csv ({len(anomaly_df):,} rows)")

# Summary by vendor flagged counts and financial exposure
anomaly_summary = (
    df.groupby("vendor_name")
    .agg(
        total_line_items    = ("purchaseorderdetailid", "count"),
        flagged_items       = ("is_anomaly", "sum"),
        total_spend         = ("line_total", "sum"),
        flagged_spend       = ("line_total", lambda x: x[df.loc[x.index, "is_anomaly"]].sum()),
        avg_z_score         = ("z_score", "mean"),
        max_z_score         = ("z_score", "max"),
    )
    .reset_index()
)
anomaly_summary["flag_rate_pct"] = (
    anomaly_summary["flagged_items"] / anomaly_summary["total_line_items"] * 100
).round(2)
anomaly_summary["flagged_spend_pct"] = (
    anomaly_summary["flagged_spend"] / anomaly_summary["total_spend"] * 100
).round(2)
anomaly_summary = anomaly_summary.sort_values("flagged_items", ascending=False)
anomaly_summary.to_csv(os.path.join(OUTPUT_DIR, "anomaly_summary.csv"), index=False)
print(f"File saved to: outputs/anomaly_summary.csv ({len(anomaly_summary):,} vendors)")