

WITH new_deal_stages_data AS (
  SELECT
    stage_id   AS deal_stage_id,
    stage_name AS deal_stage_name
  FROM "postgres"."pipedrive_snapshots"."deal_stages_snapshot"
  WHERE dbt_valid_to IS NULL
  AND stage_name IS NOT NULL
)

SELECT
  new_deal_stages_data.deal_stage_id,
  new_deal_stages_data.deal_stage_name
FROM new_deal_stages_data

