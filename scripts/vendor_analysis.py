import os
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from sqlalchemy import create_engine

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

# HELPER 
def run_query(sql, label):
    df = pd.read_sql(sql, engine)
    #print rows for logs
    print(f"{label}: {len(df):,} rows")
    return df

def save_csv(df, filename):
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    print(f"File(CSV) Saved: outputs/{filename}")
    return path

# The sqls are the same in analysis_queries.sql

# SECTION 1 — VENDOR SPEND
print("Section 1: Vendor Spend ")

# Q1: Vendor spend ranked
vendor_spend_sql = """
SELECT
    v.businessentityid                              AS vendor_id,
    v.name                                          AS vendor_name,
    v.creditrating,
    v.preferredvendorstatus                         AS is_preferred,
    COUNT(DISTINCT poh.purchaseorderid)             AS total_orders,
    SUM(pod.orderqty)                               AS total_units_ordered,
    ROUND(SUM(pod.orderqty * pod.unitprice), 2)     AS total_spend,
    ROUND(AVG(pod.unitprice), 2)                    AS avg_unit_price,
    RANK() OVER (
        ORDER BY SUM(pod.orderqty * pod.unitprice) DESC
    )                                               AS spend_rank,
    ROUND(
        SUM(SUM(pod.orderqty * pod.unitprice)) OVER (
            ORDER BY SUM(pod.orderqty * pod.unitprice) DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2
    )                                               AS running_total_spend
FROM purchasing.vendor v
JOIN purchasing.purchaseorderheader poh
    ON v.businessentityid = poh.vendorid
JOIN purchasing.purchaseorderdetail pod
    ON poh.purchaseorderid = pod.purchaseorderid
GROUP BY v.businessentityid, v.name, v.creditrating, v.preferredvendorstatus
ORDER BY spend_rank
"""
vendor_df = run_query(vendor_spend_sql, "Vendor spend ranked")
save_csv(vendor_df, "vendor_spend_summary.csv")


# Q2: Pareto analysis
pareto_sql = """
WITH vendor_spend AS (
    SELECT
        v.businessentityid                              AS vendor_id,
        v.name                                          AS vendor_name,
        ROUND(SUM(pod.orderqty * pod.unitprice), 2)     AS total_spend
    FROM purchasing.vendor v
    JOIN purchasing.purchaseorderheader poh ON v.businessentityid = poh.vendorid
    JOIN purchasing.purchaseorderdetail pod ON poh.purchaseorderid = pod.purchaseorderid
    GROUP BY v.businessentityid, v.name
),
totals AS (SELECT SUM(total_spend) AS grand_total FROM vendor_spend),
ranked AS (
    SELECT
        vs.vendor_id, vs.vendor_name, vs.total_spend,
        ROUND((vs.total_spend / t.grand_total) * 100, 2) AS spend_pct,
        ROUND(
            SUM(vs.total_spend) OVER (
                ORDER BY vs.total_spend DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / t.grand_total * 100, 2
        )                                               AS cumulative_pct,
        RANK() OVER (ORDER BY vs.total_spend DESC)      AS spend_rank
    FROM vendor_spend vs CROSS JOIN totals t
)
SELECT *, CASE WHEN cumulative_pct <= 80 THEN 'Critical Vendor'
               ELSE 'Tail Spend' END AS vendor_tier
FROM ranked ORDER BY spend_rank
"""
pareto_df = run_query(pareto_sql, "Pareto analysis")
save_csv(pareto_df, "pareto_analysis.csv")

# Q4: Monthly spend vs revenue correlation
corr_sql = """
WITH monthly_spend AS (
    SELECT
        DATE_TRUNC('month', poh.orderdate)              AS month,
        ROUND(SUM(pod.orderqty * pod.unitprice), 2)     AS total_spend
    FROM purchasing.purchaseorderheader poh
    JOIN purchasing.purchaseorderdetail pod ON poh.purchaseorderid = pod.purchaseorderid
    GROUP BY DATE_TRUNC('month', poh.orderdate)
),
monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', soh.orderdate)              AS month,
        ROUND(SUM(soh.subtotal), 2)                     AS total_revenue,
        COUNT(DISTINCT soh.salesorderid)                AS order_count
    FROM sales.salesorderheader soh
    GROUP BY DATE_TRUNC('month', soh.orderdate)
)
SELECT
    TO_CHAR(r.month, 'YYYY-MM')                         AS period,
    r.month                                             AS period_date,
    r.total_revenue,
    COALESCE(s.total_spend, 0)                          AS total_vendor_spend,
    r.order_count,
    ROUND(COALESCE(s.total_spend,0) / NULLIF(r.total_revenue,0) * 100, 2) AS spend_to_revenue_ratio_pct,
    LAG(COALESCE(s.total_spend, 0)) OVER (ORDER BY r.month) AS prior_month_spend
FROM monthly_revenue r
LEFT JOIN monthly_spend s ON r.month = s.month
ORDER BY r.month
"""
corr_df = run_query(corr_sql, "Monthly spend vs revenue")
save_csv(corr_df, "monthly_spend_vs_revenue.csv")

print("\n")
# SECTION 2 — REVENUE & MARGIN
print("Section 2: Revenue & Margin")

# Q5: Margin by category
margin_sql = """
SELECT
    pc.name                                             AS category,
    psc.name                                            AS subcategory,
    COUNT(p.productid)                                  AS product_count,
    ROUND(AVG(p.standardcost), 2)                       AS avg_standard_cost,
    ROUND(AVG(p.listprice), 2)                          AS avg_list_price,
    ROUND(AVG(p.listprice - p.standardcost), 2)         AS avg_gross_margin,
    ROUND(AVG(CASE WHEN p.listprice > 0
              THEN (p.listprice - p.standardcost) / p.listprice * 100
              ELSE NULL END), 2)                        AS avg_margin_pct,
    ROUND(MIN(p.listprice), 2)                          AS min_price,
    ROUND(MAX(p.listprice), 2)                          AS max_price
FROM production.product p
JOIN production.productsubcategory psc ON p.productsubcategoryid = psc.productsubcategoryid
JOIN production.productcategory pc ON psc.productcategoryid = pc.productcategoryid
WHERE p.listprice > 0
GROUP BY pc.name, psc.name
ORDER BY avg_margin_pct DESC
"""
margin_df = run_query(margin_sql, "Margin by category")
save_csv(margin_df, "margin_analysis.csv")

# Q6: Territory revenue MoM/YoY
territory_sql = """
WITH territory_monthly AS (
    SELECT
        st.name                                         AS territory,
        st.countryregioncode                            AS country,
        DATE_TRUNC('month', soh.orderdate)              AS period_date,
        TO_CHAR(DATE_TRUNC('month', soh.orderdate), 'YYYY-MM') AS period,
        ROUND(SUM(soh.subtotal), 2)                     AS revenue,
        COUNT(DISTINCT soh.salesorderid)                AS order_count,
        COUNT(DISTINCT soh.customerid)                  AS unique_customers
    FROM sales.salesorderheader soh
    JOIN sales.salesterritory st ON soh.territoryid = st.territoryid
    GROUP BY st.name, st.countryregioncode, DATE_TRUNC('month', soh.orderdate)
)
SELECT
    territory, country, period, revenue, order_count, unique_customers,
    LAG(revenue) OVER (PARTITION BY territory ORDER BY period_date) AS prev_month_revenue,
    ROUND((revenue - LAG(revenue) OVER (PARTITION BY territory ORDER BY period_date))
        / NULLIF(LAG(revenue) OVER (PARTITION BY territory ORDER BY period_date), 0) * 100, 2) AS mom_growth_pct,
    LAG(revenue, 12) OVER (PARTITION BY territory ORDER BY period_date) AS same_month_last_year,
    ROUND((revenue - LAG(revenue, 12) OVER (PARTITION BY territory ORDER BY period_date))
        / NULLIF(LAG(revenue, 12) OVER (PARTITION BY territory ORDER BY period_date), 0) * 100, 2) AS yoy_growth_pct
FROM territory_monthly
ORDER BY territory, period
"""
territory_df = run_query(territory_sql, "Territory revenue MoM/YoY")
save_csv(territory_df, "territory_revenue.csv")

print("\n")
# SECTION 3 — BUDGET & PERFORMANCE
print("Section 3: Budget & Performance")

# Q7: Budget vs actuals (territory YoY as proxy)
budget_sql = """
SELECT
    st.name                                             AS territory,
    st.countryregioncode                                AS country,
    st."group"                                          AS region_group,
    ROUND(st.salesytd, 2)                               AS actual_sales_ytd,
    ROUND(st.saleslastyear, 2)                          AS sales_last_year,
    ROUND(st.costytd, 2)                                AS actual_cost_ytd,
    ROUND(st.costlastyear, 2)                           AS cost_last_year,
    ROUND(st.salesytd - st.saleslastyear, 2)            AS revenue_yoy_variance,
    ROUND((st.salesytd - st.saleslastyear)
        / NULLIF(st.saleslastyear, 0) * 100, 2)         AS revenue_yoy_growth_pct,
    CASE WHEN st.salesytd > st.saleslastyear THEN 'Growing'
         WHEN st.salesytd = st.saleslastyear THEN 'Flat'
         ELSE 'Declining' END                           AS revenue_trend,
    ROUND(st.costytd - st.costlastyear, 2)              AS cost_yoy_variance,
    ROUND((st.costytd - st.costlastyear)
        / NULLIF(st.costlastyear, 0) * 100, 2)          AS cost_yoy_change_pct,
    ROUND(st.costytd / NULLIF(st.salesytd, 0) * 100, 2) AS cost_to_revenue_ratio_pct
FROM sales.salesterritory st
ORDER BY revenue_yoy_growth_pct DESC
"""
budget_df = run_query(budget_sql, "Budget vs actuals")
save_csv(budget_df, "budget_vs_actuals.csv")

# Q8: Salesperson performance
sales_perf_sql = """
SELECT
    p.firstname || ' ' || p.lastname                   AS salesperson_name,
    st.name                                             AS territory,
    ROUND(sp.salesquota, 2)                             AS quota,
    ROUND(sp.salesytd, 2)                               AS actual_ytd,
    ROUND(sp.saleslastyear, 2)                          AS last_year_sales,
    ROUND(sp.salesytd - sp.salesquota, 2)               AS variance,
    ROUND(sp.salesytd / NULLIF(sp.salesquota, 0) * 100, 2) AS attainment_pct,
    RANK() OVER (ORDER BY sp.salesytd / NULLIF(sp.salesquota, 0) DESC) AS performance_rank,
    ROUND(sp.bonus, 2)                                  AS bonus,
    ROUND(sp.commissionpct * 100, 2)                    AS commission_pct
FROM sales.salesperson sp
JOIN humanresources.employee e ON sp.businessentityid = e.businessentityid
JOIN person.person p ON e.businessentityid = p.businessentityid
LEFT JOIN sales.salesterritory st ON sp.territoryid = st.territoryid
WHERE sp.salesquota IS NOT NULL
ORDER BY attainment_pct DESC
"""
sales_perf_df = run_query(sales_perf_sql, "Salesperson performance")
save_csv(sales_perf_df, "salesperson_performance.csv")

# Q9: Promotion impact
promo_sql = """
WITH offer_revenue AS (
    SELECT
        so.specialofferid,
        so.description                                  AS offer_description,
        so.type                                         AS offer_type,
        so.category                                     AS offer_category,
        so.discountpct,
        COUNT(sod.salesorderdetailid)                   AS line_items_sold,
        SUM(sod.orderqty)                               AS total_units,
        ROUND(SUM(sod.unitprice * sod.orderqty), 2)     AS gross_revenue,
        ROUND(SUM(sod.unitprice * sod.orderqty * sod.unitpricediscount), 2) AS total_discount_given,
        ROUND(SUM(sod.unitprice * sod.orderqty * (1 - sod.unitpricediscount)), 2) AS net_revenue
    FROM sales.specialoffer so
    JOIN sales.salesorderdetail sod ON so.specialofferid = sod.specialofferid
    GROUP BY so.specialofferid, so.description, so.type, so.category, so.discountpct
)
SELECT
    offer_description, offer_type, offer_category,
    ROUND(discountpct * 100, 2)                         AS discount_pct,
    line_items_sold, total_units, gross_revenue,
    total_discount_given, net_revenue,
    ROUND(total_discount_given / NULLIF(gross_revenue, 0) * 100, 2) AS effective_discount_rate_pct,
    ROUND(net_revenue / NULLIF(line_items_sold, 0), 2)  AS revenue_per_line_item
FROM offer_revenue
ORDER BY net_revenue DESC
"""
promo_df = run_query(promo_sql, "Promotion impact")
save_csv(promo_df, "promotion_impact.csv")