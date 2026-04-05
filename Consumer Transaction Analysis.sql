-- ============================================================
-- Consumer Transaction Trend Analysis (MySQL 8+ Compatible)
-- ============================================================

-- ── TABLE SCHEMA ────────────────────────────────────────────
CREATE DATABASE consumer_analysis;
USE consumer_analysis;
CREATE TABLE IF NOT EXISTS transactions (
    txn_id          VARCHAR(50) PRIMARY KEY,
    txn_date        DATE,
    customer_id     VARCHAR(50),
    city            VARCHAR(100),
    state           VARCHAR(100),
    category        VARCHAR(100),
    payment_mode    VARCHAR(50),
    txn_amount      DECIMAL(10,2),
    is_returned     TINYINT
);

-- ── STEP 1: Data Validation ──────────────────────────────────

SELECT
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT city) AS cities_covered,
    SUM(CASE WHEN txn_amount <= 0 THEN 1 ELSE 0 END) AS invalid_amounts,
    SUM(CASE WHEN txn_date IS NULL THEN 1 ELSE 0 END) AS null_dates,
    MIN(txn_date) AS earliest_date,
    MAX(txn_date) AS latest_date
FROM transactions;

-- ── STEP 2: Monthly Revenue Trend ────────────────────────────

SELECT
    DATE_FORMAT(txn_date, '%Y-%m') AS txn_month,
    COUNT(*) AS transaction_count,
    ROUND(SUM(txn_amount), 0) AS total_revenue,
    ROUND(AVG(txn_amount), 1) AS avg_transaction_value,
    COUNT(DISTINCT customer_id) AS active_customers
FROM transactions
WHERE is_returned = 0
GROUP BY txn_month
ORDER BY txn_month;

-- ── STEP 3: Category-wise Performance ────────────────────────

SELECT
    category,
    COUNT(*) AS txn_count,
    ROUND(SUM(txn_amount), 0) AS total_revenue,
    ROUND(AVG(txn_amount), 1) AS avg_order_value,
    ROUND(100.0 * SUM(is_returned) / COUNT(*), 1) AS return_rate_pct,
    COUNT(DISTINCT customer_id) AS unique_buyers
FROM transactions
GROUP BY category
ORDER BY total_revenue DESC;

-- ── STEP 4: Payment Mode Adoption ────────────────────────────

WITH payment_counts AS (
    SELECT
        payment_mode,
        COUNT(*) AS txn_count,
        ROUND(SUM(txn_amount), 0) AS total_value,
        ROUND(AVG(txn_amount), 1) AS avg_txn_value
    FROM transactions
    GROUP BY payment_mode
),
grand_total AS (
    SELECT SUM(txn_count) AS all_txns FROM payment_counts
)
SELECT
    p.payment_mode,
    p.txn_count,
    ROUND(100.0 * p.txn_count / g.all_txns, 1) AS share_pct,
    p.total_value,
    p.avg_txn_value
FROM payment_counts p
CROSS JOIN grand_total g
ORDER BY p.txn_count DESC;

-- ── STEP 5: City-wise Revenue Contribution ───────────────────

WITH city_revenue AS (
    SELECT
        city,
        state,
        COUNT(*) AS txn_count,
        COUNT(DISTINCT customer_id) AS unique_customers,
        ROUND(SUM(txn_amount), 0) AS total_revenue,
        ROUND(AVG(txn_amount), 1) AS avg_order_value
    FROM transactions
    WHERE is_returned = 0
    GROUP BY city, state
),
grand_total AS (
    SELECT SUM(total_revenue) AS all_revenue FROM city_revenue
)
SELECT
    c.city,
    c.state,
    c.txn_count,
    c.unique_customers,
    c.total_revenue,
    ROUND(100.0 * c.total_revenue / g.all_revenue, 1) AS revenue_share_pct,
    c.avg_order_value
FROM city_revenue c
CROSS JOIN grand_total g
ORDER BY c.total_revenue DESC
LIMIT 15;

-- ── STEP 6: Customer Segmentation (RFM Proxy) ────────────────

WITH customer_stats AS (
    SELECT
        customer_id,
        COUNT(*) AS frequency,
        ROUND(SUM(txn_amount), 0) AS monetary,
        MAX(txn_date) AS last_purchase_date,
        MIN(txn_date) AS first_purchase_date
    FROM transactions
    WHERE is_returned = 0
    GROUP BY customer_id
)
SELECT
    customer_id,
    frequency,
    monetary,
    last_purchase_date,
    first_purchase_date,
    CASE
        WHEN frequency >= 10 AND monetary >= 50000 THEN 'Champions'
        WHEN frequency >= 5 AND monetary >= 20000 THEN 'Loyal Customers'
        WHEN frequency >= 3 THEN 'Potential Loyalist'
        WHEN frequency = 1 THEN 'One-Time Buyer'
        ELSE 'At-Risk'
    END AS customer_segment
FROM customer_stats
ORDER BY monetary DESC;

-- ── STEP 7: Year-over-Year Growth ────────────────────────────

WITH yearly AS (
    SELECT
        category,
        YEAR(txn_date) AS yr,
        ROUND(SUM(txn_amount), 0) AS revenue
    FROM transactions
    WHERE is_returned = 0
    GROUP BY category, yr
)
SELECT
    a.category,
    a.revenue AS revenue_2023,
    b.revenue AS revenue_2024,
    ROUND(100.0 * (b.revenue - a.revenue) / a.revenue, 1) AS yoy_growth_pct
FROM yearly a
JOIN yearly b
    ON a.category = b.category
    AND a.yr = 2023
    AND b.yr = 2024
ORDER BY yoy_growth_pct DESC;

-- ── STEP 8: Return Rate Analysis ─────────────────────────────

SELECT
    category,
    payment_mode,
    COUNT(*) AS total_txns,
    SUM(is_returned) AS returned,
    ROUND(100.0 * SUM(is_returned) / COUNT(*), 1) AS return_rate_pct,
    ROUND(AVG(txn_amount), 1) AS avg_txn_value
FROM transactions
GROUP BY category, payment_mode
HAVING COUNT(*) >= 10
ORDER BY return_rate_pct DESC;

-- ── STEP 9: Day-of-Week Pattern ──────────────────────────────

SELECT
    CASE DAYOFWEEK(txn_date)
        WHEN 1 THEN '1_Sunday'
        WHEN 2 THEN '2_Monday'
        WHEN 3 THEN '3_Tuesday'
        WHEN 4 THEN '4_Wednesday'
        WHEN 5 THEN '5_Thursday'
        WHEN 6 THEN '6_Friday'
        WHEN 7 THEN '7_Saturday'
    END AS day_of_week,
    COUNT(*) AS txn_count,
    ROUND(SUM(txn_amount), 0) AS total_revenue,
    ROUND(AVG(txn_amount), 1) AS avg_order_value
FROM transactions
WHERE is_returned = 0
GROUP BY DAYOFWEEK(txn_date)
ORDER BY DAYOFWEEK(txn_date);