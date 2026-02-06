SELECT 
    funnel_month as "month",
    funnel_stage_name as "kpi_name",
    funnel_step as "funnel_step",
    deals_in_stage as "deals_count"

FROM "postgres"."public_sales_analytics_intermediate"."int_sales_funnel_monthly"
WHERE funnel_step IN ('1', '2', '2.1', '3', '3.1', '4', '5', '6', '7', '8', '9', '10')