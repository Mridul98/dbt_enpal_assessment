
WITH deal_changes AS (
  SELECT
    deal_id,
    change_time,
    stage_id,
    deal_lost_reason_id

  FROM "postgres"."public_pipedrive_staging"."stg_deal_changes"
  WHERE stage_id IS NOT NULL
)

SELECT
  deal_changes.*,
  stages.deal_stage_name,
  deal_lost_reason.actual_lost_reason,
  FIRST_VALUE(deal_changes.change_time) OVER (PARTITION BY deal_changes.deal_id, deal_changes.stage_id ORDER BY deal_changes.change_time) AS first_entry_date_for_stage
FROM deal_changes
INNER JOIN "postgres"."public_pipedrive_staging"."stg_deal_stages" AS stages
  ON deal_changes.stage_id = stages.deal_stage_id
LEFT JOIN "postgres"."public_pipedrive_staging"."stg_deal_lost_reason" AS deal_lost_reason
  ON deal_changes.deal_lost_reason_id = deal_lost_reason.lost_reason_id