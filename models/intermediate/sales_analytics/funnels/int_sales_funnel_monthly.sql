{{
    config(
        materialized='incremental',
        unique_key=['funnel_month', 'funnel_step'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['funnel_month'], 'type': 'btree'},
            {'columns': ['funnel_month', 'funnel_step'], 'type': 'btree'}
        ],
    )
}}

WITH sales_funnel AS (
  SELECT
    deal_id,
    funnel_stage_name,
    funnel_step,
    funnel_month
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
