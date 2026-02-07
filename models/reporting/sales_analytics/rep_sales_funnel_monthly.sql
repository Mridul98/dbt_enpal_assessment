SELECT 
    monthly_sales_funnel.funnel_month as "month",
    kpi_mapping.funnel_stage_name as kpi_name,
    monthly_sales_funnel.funnel_step as funnel_step,
    monthly_sales_funnel.deals_in_stage as deals_count

FROM {{ ref('int_sales_funnel_monthly') }} monthly_sales_funnel
INNER JOIN {{ ref('int_funnel_kpi_mapping') }} AS kpi_mapping
  ON monthly_sales_funnel.funnel_step = kpi_mapping.funnel_step