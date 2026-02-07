{{
    config(
        materialized = 'incremental',
        unique_key = ['deal_id', 'stage_id'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
     )
}}
WITH first_stage_entry AS (
  SELECT
    dc.deal_id,
    dc.stage_id,
    MIN(dc.change_time) AS first_entry_date_for_stage
  FROM "postgres"."dev_pipedrive_staging"."stg_pipedrive_deal_changes" dc
  WHERE dc.stage_id IS NOT NULL
  GROUP BY
    dc.deal_id,
    dc.stage_id
)

SELECT
  fse.deal_id,
  fse.stage_id,
  fse.first_entry_date_for_stage,
  s.deal_stage_name
FROM first_stage_entry fse
JOIN "postgres"."dev_pipedrive_staging"."stg_pipedrive_deal_stages" s
  ON fse.stage_id = s.deal_stage_id
{% if is_incremental() %}
WHERE NOT EXISTS (
  SELECT 1
  FROM "postgres"."dev_sales_analytics_intermediate"."int_deal_stage_progression" p
  WHERE
    p.deal_id  = fse.deal_id
    AND p.stage_id = fse.stage_id
)
{% endif %}