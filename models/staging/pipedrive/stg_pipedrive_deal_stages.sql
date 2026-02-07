SELECT
  stage_id   AS deal_stage_id,
  stage_name AS deal_stage_name
FROM {{ ref('pipedrive_deal_stages_snapshot') }}
WHERE
  dbt_valid_to IS NULL
  AND stage_name IS NOT NULL

