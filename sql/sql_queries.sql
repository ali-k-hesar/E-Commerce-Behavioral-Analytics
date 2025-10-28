-- 1 — Customer value & churn: who to keep, who to win back, and where to focus marketing spend

-- Who are our highest-value customers by purchase frequency and basket size (orders, items/order, reorder-rate)?
-- Who is “at risk” of churn (long gap since last order relative to their personal cadence)?
-- Which user segments (by behavior) give the biggest ROI if we target them (e.g., frequent small baskets vs. infrequent big baskets)?
-- What retention thresholds (days without order) should trigger re-engagement campaigns?

-- 1. Customer metrics & segments (uses cumulative days to reconstruct relative timeline per user)
WITH user_orders AS (
  SELECT
    o.*,
    SUM(COALESCE(o.days_since_prior_order, 0)) OVER (PARTITION BY o.user_id ORDER BY o.order_number) AS days_since_first_order
  FROM public.orders o
)
, user_order_items AS (
  SELECT
    u.user_id,
    u.order_id,
    u.order_number,
    u.days_since_first_order,
    u.days_since_prior_order,
    COUNT(op.product_id) AS items_in_order,
    SUM(op.reordered::INT) AS reordered_in_order
  FROM user_orders u
  JOIN public.order_products op ON op.order_id = u.order_id
  GROUP BY 1, 2, 3, 4, 5
)
, user_medians AS (
  SELECT
    user_id,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_since_prior_order) AS median_days_between_orders
  FROM public.orders
  WHERE days_since_prior_order IS NOT NULL
  GROUP BY user_id
)
, user_sub_summary AS (
  SELECT
    uo.user_id,
    COUNT(*) AS total_orders,
    AVG(items_in_order)::NUMERIC(10,2) AS avg_items_per_order,
    SUM(items_in_order) AS total_items,
    SUM(reordered_in_order)::NUMERIC(10,4) / NULLIF(SUM(items_in_order), 0) AS overall_reorder_rate,
    MAX(days_since_first_order) AS days_from_first_to_last,
    (array_agg(uo.days_since_prior_order ORDER BY uo.order_number DESC))[1] AS days_since_last_order  FROM user_order_items uo
  GROUP BY uo.user_id
)
, user_summary AS (
  SELECT
    s.user_id,
    s.total_orders,
    s.avg_items_per_order,
    s.total_items,
    s.overall_reorder_rate,
    s.days_from_first_to_last,
    s.days_since_last_order, 
    m.median_days_between_orders,
    NTILE(20) OVER (ORDER BY s.total_items DESC) AS decile_by_items 
  FROM user_sub_summary s
  LEFT JOIN user_medians m USING (user_id)
)
SELECT
  us.user_id,
  us.total_orders,
  us.avg_items_per_order,
  us.total_items,
  us.overall_reorder_rate,
  us.days_from_first_to_last,
  us.median_days_between_orders,
  us.days_since_last_order,
  us.decile_by_items,
  CASE WHEN us.decile_by_items = 1 THEN 'high_value' ELSE 'normal' END AS value_segment,
  CASE
    WHEN us.median_days_between_orders IS NULL THEN 'insufficient_history'
    WHEN us.days_since_last_order > (us.median_days_between_orders * 2) THEN 'at_risk'
    ELSE 'active'
  END AS retention_flag
FROM user_summary us
ORDER BY us.total_items DESC
LIMIT 100;

-- 2 — Product affinity & cross-sell: which product pairs and clusters to promote together
-- Which product pairs co-occur in baskets significantly more than by chance (support, confidence, lift)?
-- Which aisles/departments are most likely to be bought together?
-- Which cross-sell pairings would increase basket size with minimal discounting?
-- Are there “gateway” SKUs that appear in many baskets and act as hooks for other purchases?

-- Top product pairs with support, confidence and lift
WITH orders_products AS (
  SELECT order_id, product_id FROM order_products
),
order_counts AS (
  SELECT order_id, COUNT(*) AS items_in_order FROM orders_products GROUP BY order_id
),
product_order_count AS (
  SELECT product_id, COUNT(DISTINCT order_id) AS orders_with_product FROM orders_products GROUP BY product_id
),
total_orders AS (
  SELECT COUNT(DISTINCT order_id) AS total_orders FROM orders_products
),
pairs AS (
  SELECT
    p1.product_id AS prod_a,
    p2.product_id AS prod_b,
    COUNT(DISTINCT p1.order_id) AS co_orders
  FROM orders_products p1
  JOIN orders_products p2
    ON p1.order_id = p2.order_id
   AND p1.product_id < p2.product_id
  GROUP BY p1.product_id, p2.product_id
),
pairs_stats AS (
  SELECT
    pr.prod_a,
    pr.prod_b,
    pr.co_orders,
    poc_a.orders_with_product AS orders_a,
    poc_b.orders_with_product AS orders_b,
    t.total_orders,
    (pr.co_orders::FLOAT / t.total_orders) AS support,                                    -- P(A&B)
    (pr.co_orders::FLOAT / poc_a.orders_with_product) AS confidence_a_to_b,               -- P(B|A)
    (pr.co_orders::FLOAT / poc_b.orders_with_product) AS confidence_b_to_a,               -- P(A|B)
    ( (pr.co_orders::FLOAT / t.total_orders) / ( (poc_a.orders_with_product::FLOAT / t.total_orders) * (poc_b.orders_with_product::FLOAT / t.total_orders) ) ) AS lift
  FROM pairs pr
  JOIN product_order_count poc_a ON poc_a.product_id = pr.prod_a
  JOIN product_order_count poc_b ON poc_b.product_id = pr.prod_b
  CROSS JOIN total_orders t
)
SELECT ps.*,
       pa.product_name AS product_a_name,
       pb.product_name AS product_b_name,
       da.department AS dep_a,
       db.department AS dep_b,
       aa.aisle AS aisle_a,
       ab.aisle AS aisle_b
FROM pairs_stats ps
JOIN products pa ON pa.product_id = ps.prod_a
JOIN products pb ON pb.product_id = ps.prod_b
JOIN departments da ON da.department_id = pa.department_id
JOIN departments db ON db.department_id = pb.department_id
JOIN aisles aa ON aa.aisle_id = pa.aisle_id
JOIN aisles ab ON ab.aisle_id = pb.aisle_id
WHERE ps.co_orders >= 50 -- filter noise; tune threshold to your dataset size
ORDER BY ps.lift DESC, ps.co_orders DESC
LIMIT 100;


-- 3 — SKU & assortment optimization: which SKUs to delist, which to promote
-- Which SKUs are long tail (few orders, few unique buyers) and produce little reorder value?
-- Which SKUs dominate a department’s sales (Pareto) — can we reduce SKUs and keep revenue?
-- Which SKUs have high reorder-rate but low unique buyers (single-users repeatedly buying) vs. broad appeal?
-- Within each aisle/department, which SKUs to promote vs. delist?

-- SKU performance and Pareto partitioning per department
WITH sku_stats AS (
  SELECT
    p.product_id,
    p.product_name,
    p.aisle_id,
    p.department_id,
    COUNT(op.order_id) AS total_sales_count,
    COUNT(DISTINCT op.order_id) AS orders_with_sku,
    COUNT(DISTINCT o.user_id) AS unique_buyers,
    SUM(CASE WHEN op.reordered THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(op.order_id),0) AS reorder_rate,
    AVG(op.add_to_cart_order)::FLOAT AS avg_add_to_cart_position
  FROM products p
  LEFT JOIN order_products op ON op.product_id = p.product_id
  LEFT JOIN orders o ON o.order_id = op.order_id
  GROUP BY p.product_id, p.product_name, p.aisle_id, p.department_id
),
department_totals AS (
  SELECT department_id, SUM(total_sales_count) AS dept_total_sales
  FROM sku_stats
  GROUP BY department_id
),
sku_ranked AS (
  SELECT
    s.*,
    d.dept_total_sales,
    s.total_sales_count::FLOAT / d.dept_total_sales AS dept_sales_share,
    RANK() OVER (PARTITION BY s.department_id ORDER BY s.total_sales_count DESC) AS dept_rank,
    SUM(s.total_sales_count) OVER (PARTITION BY s.department_id ORDER BY s.total_sales_count DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sales
  FROM sku_stats s
  JOIN department_totals d ON d.department_id = s.department_id
)
SELECT
  sk.*,
  (cumulative_sales::FLOAT / dept_total_sales) AS cumulative_share
FROM sku_ranked sk
ORDER BY department_id, dept_rank
LIMIT 200;


-- 4 — Operational peaks & picking efficiency: staffing and fulfillment decisions
-- When are peak order volumes and peak item-picks (hour-of-day × day-of-week), and how many picking staff do we need?
-- How does average items per order vary by hour/day — are peak hours also high-cart-size?
-- Which products are consistently early in carts (low add_to_cart_order) — useful for routing pick-paths and clustering in picker zones?
-- Which orders have many slow-moving items (many distinct aisles) and need special handling?

-- 1) Orders per day-of-week and hour; avg items per order
WITH order_item_counts AS (
  SELECT
    o.order_id,
    o.order_dow,
    o.order_hour_of_day,
    COUNT(op.product_id) AS items_in_order,
    COUNT(DISTINCT p.aisle_id) AS distinct_aisles_in_order
  FROM orders o
  JOIN order_products op ON op.order_id = o.order_id
  JOIN products p ON p.product_id = op.product_id
  GROUP BY o.order_id, o.order_dow, o.order_hour_of_day
),
hourly AS (
  SELECT
    order_dow,
    order_hour_of_day,
    COUNT(order_id) AS orders_count,
    AVG(items_in_order)::NUMERIC(10,2) AS avg_items_per_order,
    AVG(distinct_aisles_in_order)::NUMERIC(10,2) AS avg_distinct_aisles
  FROM order_item_counts
  GROUP BY order_dow, order_hour_of_day
),
product_positions AS (
  SELECT
    op.product_id,
    AVG(op.add_to_cart_order)::NUMERIC(10,2) AS avg_add_to_cart_pos,
    COUNT(*) AS occurrences
  FROM order_products op
  GROUP BY op.product_id
)
SELECT * FROM hourly ORDER BY order_dow, order_hour_of_day;
-- Use product_positions separately for picking routing decisions


-- 5 — Product reorder/personalization model features: build the training set & feature engine
-- What feature set can we extract from the raw tables (user & product history) to predict whether a user will reorder a product in their next order?
-- How to generate per-user×product labeled training rows with historical features (usage counts, last-seen days, avg add-to-cart position, reorder rate)?
-- Which features are strongest candidates for a simple rules-based recommender before ML (recency, freq, basket affinity)?

-- Build training rows at user×product×order granularity: feature snapshot before each order, label = product present in next order
-- Note: expensive on 33M orders — run as batch job; include indexes on (user_id, order_number) and (product_id).
WITH u AS (
  SELECT o.order_id, o.user_id, o.order_number,
    SUM(COALESCE(o.days_since_prior_order,0)) OVER (PARTITION BY o.user_id ORDER BY o.order_number) AS days_since_first_order
  FROM orders o
)
, ops AS (
  SELECT
    o.user_id,
    o.order_id,
    o.order_number,
    op.product_id,
    op.add_to_cart_order,
    op.reordered
  FROM u o
  JOIN order_products op ON op.order_id = o.order_id
)
-- For each user/product/order, produce features aggregated up to that order (exclude the current order when computing historical features)
, hist AS (
  SELECT
    op.user_id,
    op.product_id,
    op.order_number AS snapshot_order_number,
    -- historical counts up to previous order
    COALESCE(SUM(CASE WHEN op2.order_number < op.order_number THEN 1 ELSE 0 END) OVER (PARTITION BY op.user_id, op.product_id ORDER BY op.order_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS times_seen_before,
    COALESCE(SUM(CASE WHEN op2.order_number < op.order_number AND op2.reordered THEN 1 ELSE 0 END) OVER (PARTITION BY op.user_id, op.product_id ORDER BY op.order_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) AS times_reordered_before,
    AVG(CASE WHEN op2.order_number < op.order_number THEN op2.add_to_cart_order END) OVER (PARTITION BY op.user_id, op.product_id ORDER BY op.order_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_add_to_cart_before,
    -- last time seen in orders before this snapshot (in days since first)
    MAX(CASE WHEN op2.order_number < op.order_number THEN o2.days_since_prior_order END) OVER (PARTITION BY op.user_id, op.product_id ORDER BY op.order_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS last_seen_days_since_first
  FROM ops op
  LEFT JOIN ops op2 ON op2.user_id = op.user_id AND op2.product_id = op.product_id
  LEFT JOIN orders o2 ON o2.order_id = op2.order_id
)
-- Label: whether this product appears in user's NEXT order after snapshot_order_number
, next_order_label AS (
  SELECT
    h.user_id,
    h.product_id,
    h.snapshot_order_number,
    CASE WHEN EXISTS (
      SELECT 1 FROM orders o_next
      JOIN order_products opn ON opn.order_id = o_next.order_id
      WHERE o_next.user_id = h.user_id AND o_next.order_number = h.snapshot_order_number + 1 AND opn.product_id = h.product_id
    ) THEN 1 ELSE 0 END AS label_next_order
  FROM hist h
)
-- Combine features + label
SELECT
  h.user_id,
  h.product_id,
  h.snapshot_order_number,
  h.times_seen_before,
  h.times_reordered_before,
  COALESCE(h.avg_add_to_cart_before, 999) AS avg_add_to_cart_before,
  h.last_seen_days_since_first,
  n.label_next_order
FROM hist h
JOIN next_order_label n USING (user_id, product_id, snapshot_order_number)
WHERE h.times_seen_before IS NOT NULL -- filter to rows with history (or include zeros)
LIMIT 200000; -- produce a sample; remove limit to build full dataset (heavy)

