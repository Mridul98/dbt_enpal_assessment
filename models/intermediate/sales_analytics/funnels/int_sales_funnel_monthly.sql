WITH sales_funnel AS (
  SELECT
    deal_id,
    funnel_stage_name,
    funnel_step,
    (DATE_TRUNC('MONTH', first_entry_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS funnel_month
  FROM {{ ref('int_sales_funnel') }}
)

SELECT
  funnel_month,
  funnel_stage_name,
  funnel_step,
  COUNT(DISTINCT deal_id) AS deals_in_stage
FROM sales_funnel
GROUP BY
  funnel_month,
  funnel_stage_name,
  funnel_step
