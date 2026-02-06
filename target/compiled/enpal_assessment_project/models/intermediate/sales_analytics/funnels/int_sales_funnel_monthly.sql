WITH sales_funnel as (
    SELECT
        deal_id,
        funnel_stage_name,
        funnel_step,
        (date_trunc('MONTH', first_entry_date) + interval '1 month' - interval '1 day')::date AS funnel_month
    FROM "postgres"."public_sales_analytics_intermediate"."int_sales_funnel"
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