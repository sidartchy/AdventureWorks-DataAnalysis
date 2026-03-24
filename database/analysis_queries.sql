-- SQL Analytical Layer
-- Database: Adventureworks
-- Queries : 10

-- Q1: Total Vendor Spend — Ranked with Running Total
SELECT
    v.businessentityid                                  AS vendor_id,
    v.name                                              AS vendor_name,
    v.creditrating,
    v.preferredvendorstatus                             AS is_preferred,
    COUNT(DISTINCT poh.purchaseorderid)                 AS total_orders,
    SUM(pod.orderqty)                                   AS total_units_ordered,
    ROUND(SUM(pod.orderqty * pod.unitprice), 2)         AS total_spend,
    ROUND(AVG(pod.unitprice), 2)                        AS avg_unit_price,
    RANK() OVER (
        ORDER BY SUM(pod.orderqty * pod.unitprice) DESC
    )                                                   AS spend_rank,
    ROUND(
        SUM(SUM(pod.orderqty * pod.unitprice)) OVER (
            ORDER BY SUM(pod.orderqty * pod.unitprice) DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2
    )                                                   AS running_total_spend
FROM purchasing.vendor v
JOIN purchasing.purchaseorderheader poh
    ON v.businessentityid = poh.vendorid
JOIN purchasing.purchaseorderdetail pod
    ON poh.purchaseorderid = pod.purchaseorderid
GROUP BY
    v.businessentityid, v.name,
    v.creditrating, v.preferredvendorstatus
ORDER BY spend_rank;


-- Q2: Vendor Pareto Analysis (80/20 Rule)
WITH vendor_spend AS (
    SELECT
        v.businessentityid                              AS vendor_id,
        v.name                                          AS vendor_name,
        ROUND(SUM(pod.orderqty * pod.unitprice), 2)     AS total_spend
    FROM purchasing.vendor v
    JOIN purchasing.purchaseorderheader poh
        ON v.businessentityid = poh.vendorid
    JOIN purchasing.purchaseorderdetail pod
        ON poh.purchaseorderid = pod.purchaseorderid
    GROUP BY v.businessentityid, v.name
),
totals AS (
    SELECT SUM(total_spend) AS grand_total FROM vendor_spend
),
ranked AS (
    SELECT
        vs.vendor_id,
        vs.vendor_name,
        vs.total_spend,
        ROUND((vs.total_spend / t.grand_total) * 100, 2)   AS spend_pct,
        ROUND(
            SUM(vs.total_spend) OVER (
                ORDER BY vs.total_spend DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / t.grand_total * 100, 2
        )                                                   AS cumulative_pct,
        RANK() OVER (ORDER BY vs.total_spend DESC)          AS spend_rank
    FROM vendor_spend vs
    CROSS JOIN totals t
)
SELECT
    spend_rank,
    vendor_id,
    vendor_name,
    total_spend,
    spend_pct,
    cumulative_pct,
    CASE
        WHEN cumulative_pct <= 80 THEN 'Critical Vendor'
        ELSE 'Tail Spend'
    END                                                     AS vendor_tier
FROM ranked
ORDER BY spend_rank;


-- Q3: Purchase Order Status Breakdown by Vendor
-- Status: 1=Pending, 2=Approved, 3=Rejected, 4=Complete
SELECT
    v.name                                              AS vendor_name,
    v.creditrating,
    COUNT(poh.purchaseorderid)                          AS total_pos,
    SUM(CASE WHEN poh.status = 1 THEN 1 ELSE 0 END)    AS pending,
    SUM(CASE WHEN poh.status = 2 THEN 1 ELSE 0 END)    AS approved,
    SUM(CASE WHEN poh.status = 3 THEN 1 ELSE 0 END)    AS rejected,
    SUM(CASE WHEN poh.status = 4 THEN 1 ELSE 0 END)    AS complete,
    ROUND(
        SUM(CASE WHEN poh.status = 3 THEN 1 ELSE 0 END)::NUMERIC
        / COUNT(poh.purchaseorderid) * 100, 2
    )                                                   AS rejection_rate_pct,
    ROUND(SUM(poh.subtotal), 2)                         AS total_po_value,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (poh.shipdate - poh.orderdate)) / 86400
    ), 1)                                               AS avg_lead_time_days
FROM purchasing.vendor v
JOIN purchasing.purchaseorderheader poh
    ON v.businessentityid = poh.vendorid
GROUP BY v.name, v.creditrating
ORDER BY total_po_value DESC;


-- Q4: Monthly Vendor Spend vs Sales Revenue Correlation
WITH monthly_spend AS (
    SELECT
        DATE_TRUNC('month', poh.orderdate)              AS month,
        ROUND(SUM(pod.orderqty * pod.unitprice), 2)     AS total_spend
    FROM purchasing.purchaseorderheader poh
    JOIN purchasing.purchaseorderdetail pod
        ON poh.purchaseorderid = pod.purchaseorderid
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
    r.total_revenue,
    COALESCE(s.total_spend, 0)                          AS total_vendor_spend,
    r.order_count,
    ROUND(
        COALESCE(s.total_spend, 0) / NULLIF(r.total_revenue, 0) * 100
    , 2)                                                AS spend_to_revenue_ratio_pct,
    LAG(COALESCE(s.total_spend, 0)) OVER (
        ORDER BY r.month
    )                                                   AS prior_month_spend
FROM monthly_revenue r
LEFT JOIN monthly_spend s ON r.month = s.month
ORDER BY r.month;


-- Q5: Product Margin Analysis by Category
SELECT
    pc.name                                             AS category,
    psc.name                                            AS subcategory,
    COUNT(p.productid)                                  AS product_count,
    ROUND(AVG(p.standardcost), 2)                       AS avg_standard_cost,
    ROUND(AVG(p.listprice), 2)                          AS avg_list_price,
    ROUND(AVG(p.listprice - p.standardcost), 2)         AS avg_gross_margin,
    ROUND(
        AVG(
            CASE
                WHEN p.listprice > 0
                THEN (p.listprice - p.standardcost) / p.listprice * 100
                ELSE NULL
            END
        ), 2
    )                                                   AS avg_margin_pct,
    ROUND(MIN(p.listprice), 2)                          AS min_price,
    ROUND(MAX(p.listprice), 2)                          AS max_price
FROM production.product p
JOIN production.productsubcategory psc
    ON p.productsubcategoryid = psc.productsubcategoryid
JOIN production.productcategory pc
    ON psc.productcategoryid = pc.productcategoryid
WHERE p.listprice > 0
GROUP BY pc.name, psc.name
ORDER BY avg_margin_pct DESC;


-- Q6: Sales Revenue by Territory — MoM and YoY Growth
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
    JOIN sales.salesterritory st
        ON soh.territoryid = st.territoryid
    GROUP BY st.name, st.countryregioncode,
             DATE_TRUNC('month', soh.orderdate)
)
SELECT
    territory,
    country,
    period,
    revenue,
    order_count,
    unique_customers,
    LAG(revenue) OVER (
        PARTITION BY territory ORDER BY period_date
    )                                                   AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (
            PARTITION BY territory ORDER BY period_date
        )) / NULLIF(LAG(revenue) OVER (
            PARTITION BY territory ORDER BY period_date
        ), 0) * 100
    , 2)                                                AS mom_growth_pct,
    LAG(revenue, 12) OVER (
        PARTITION BY territory ORDER BY period_date
    )                                                   AS same_month_last_year,
    ROUND(
        (revenue - LAG(revenue, 12) OVER (
            PARTITION BY territory ORDER BY period_date
        )) / NULLIF(LAG(revenue, 12) OVER (
            PARTITION BY territory ORDER BY period_date
        ), 0) * 100
    , 2)                                                AS yoy_growth_pct
FROM territory_monthly
ORDER BY territory, period;


-- Q7: Territory Revenue & Cost — YoY Variance Analysis
SELECT
    st.name                                             AS territory,
    st.countryregioncode                                AS country,
    st."group"                                          AS region_group,
    ROUND(st.salesytd, 2)                               AS actual_sales_ytd,
    ROUND(st.saleslastyear, 2)                          AS sales_last_year,
    ROUND(st.costytd, 2)                                AS actual_cost_ytd,
    ROUND(st.costlastyear, 2)                           AS cost_last_year,
    ROUND(st.salesytd - st.saleslastyear, 2)            AS revenue_yoy_variance,
    ROUND(
        (st.salesytd - st.saleslastyear)
        / NULLIF(st.saleslastyear, 0) * 100
    , 2)                                                AS revenue_yoy_growth_pct,
    CASE
        WHEN st.salesytd > st.saleslastyear             THEN 'Growing'
        WHEN st.salesytd = st.saleslastyear             THEN 'Flat'
        ELSE 'Declining'
    END                                                 AS revenue_trend,
    ROUND(st.costytd - st.costlastyear, 2)              AS cost_yoy_variance,
    ROUND(
        (st.costytd - st.costlastyear)
        / NULLIF(st.costlastyear, 0) * 100
    , 2)                                                AS cost_yoy_change_pct,
    ROUND(
        st.costytd / NULLIF(st.salesytd, 0) * 100, 2
    )                                                   AS cost_to_revenue_ratio_pct
FROM sales.salesterritory st
ORDER BY revenue_yoy_growth_pct DESC;


-- Q8: Salesperson Quota Attainment & Performance Ranking
SELECT
    p.firstname || ' ' || p.lastname                   AS salesperson_name,
    st.name                                             AS territory,
    ROUND(sp.salesquota, 2)                             AS quota,
    ROUND(sp.salesytd, 2)                               AS actual_ytd,
    ROUND(sp.saleslastyear, 2)                          AS last_year_sales,
    ROUND(sp.salesytd - sp.salesquota, 2)               AS variance,
    ROUND(
        sp.salesytd / NULLIF(sp.salesquota, 0) * 100
    , 2)                                                AS attainment_pct,
    RANK() OVER (
        ORDER BY sp.salesytd / NULLIF(sp.salesquota, 0) DESC
    )                                                   AS performance_rank,
    ROUND(sp.bonus, 2)                                  AS bonus,
    ROUND(sp.commissionpct * 100, 2)                    AS commission_pct
FROM sales.salesperson sp
JOIN humanresources.employee e
    ON sp.businessentityid = e.businessentityid
JOIN person.person p
    ON e.businessentityid = p.businessentityid
LEFT JOIN sales.salesterritory st
    ON sp.territoryid = st.territoryid
WHERE sp.salesquota IS NOT NULL
ORDER BY attainment_pct DESC;


-- Q9: Discount & Promotion Impact on Revenue
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
        ROUND(SUM(
            sod.unitprice * sod.orderqty * sod.unitpricediscount
        ), 2)                                           AS total_discount_given,
        ROUND(SUM(
            sod.unitprice * sod.orderqty * (1 - sod.unitpricediscount)
        ), 2)                                           AS net_revenue
    FROM sales.specialoffer so
    JOIN sales.salesorderdetail sod
        ON so.specialofferid = sod.specialofferid
    GROUP BY so.specialofferid, so.description,
             so.type, so.category, so.discountpct
)
SELECT
    offer_description,
    offer_type,
    offer_category,
    ROUND(discountpct * 100, 2)                         AS discount_pct,
    line_items_sold,
    total_units,
    gross_revenue,
    total_discount_given,
    net_revenue,
    ROUND(
        total_discount_given / NULLIF(gross_revenue, 0) * 100
    , 2)                                                AS effective_discount_rate_pct,
    ROUND(
        net_revenue / NULLIF(line_items_sold, 0), 2
    )                                                   AS revenue_per_line_item
FROM offer_revenue
ORDER BY net_revenue DESC;


-- Q10: Anomaly Detection Prep — Price Deviation Flagging
WITH vendor_product_avg AS (
    SELECT
        poh.vendorid,
        pod.productid,
        ROUND(AVG(pod.unitprice), 4)                    AS avg_unit_price,
        ROUND(STDDEV(pod.unitprice), 4)                 AS stddev_unit_price,
        COUNT(*)                                        AS transaction_count
    FROM purchasing.purchaseorderheader poh
    JOIN purchasing.purchaseorderdetail pod
        ON poh.purchaseorderid = pod.purchaseorderid
    GROUP BY poh.vendorid, pod.productid
    HAVING COUNT(*) >= 2
)
SELECT
    poh.purchaseorderid,
    pod.purchaseorderdetailid,
    v.name                                              AS vendor_name,
    p.name                                              AS product_name,
    pc.name                                             AS category,
    pod.orderqty,
    pod.unitprice                                       AS paid_unit_price,
    p.standardcost                                      AS standard_cost,
    vpa.avg_unit_price                                  AS vendor_avg_price,
    vpa.stddev_unit_price,
    ROUND(pod.unitprice - vpa.avg_unit_price, 4)        AS price_deviation,
    ROUND(
        CASE
            WHEN vpa.stddev_unit_price > 0
            THEN (pod.unitprice - vpa.avg_unit_price)
                 / vpa.stddev_unit_price
            ELSE 0
        END
    , 2)                                                AS z_score,
    ROUND(pod.orderqty * pod.unitprice, 2)              AS line_total,
    TO_CHAR(poh.orderdate, 'YYYY-MM-DD')                AS order_date,
    CASE
        WHEN ABS(
            CASE
                WHEN vpa.stddev_unit_price > 0
                THEN (pod.unitprice - vpa.avg_unit_price)
                     / vpa.stddev_unit_price
                ELSE 0
            END
        ) > 2 THEN 'FLAG'
        ELSE 'Normal'
    END                                                 AS anomaly_flag
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
LEFT JOIN vendor_product_avg vpa
    ON poh.vendorid = vpa.vendorid
    AND pod.productid = vpa.productid
ORDER BY ABS(
    CASE
        WHEN vpa.stddev_unit_price > 0
        THEN (pod.unitprice - vpa.avg_unit_price)
             / vpa.stddev_unit_price
        ELSE 0
    END
) DESC;