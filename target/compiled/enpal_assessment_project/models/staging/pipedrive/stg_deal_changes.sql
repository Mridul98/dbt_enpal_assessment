
WITH marked_deal_changes AS (
  SELECT
    deal_id,
    change_time,
    changed_field_key,
    new_value,
    CASE
      WHEN changed_field_key = 'stage_id' THEN new_value::INT
    END AS stage_id,
    CASE
      WHEN changed_field_key = 'user_id' THEN new_value::INT
    END AS user_id,
    CASE
      WHEN changed_field_key = 'add_time' THEN new_value::TIMESTAMP
    END AS deal_created_at,
    CASE
      WHEN changed_field_key = 'lost_reason' THEN new_value::TEXT
    END AS deal_lost_reason
  FROM
    "postgres"."public"."deal_changes"
)

SELECT
  deal_id,
  change_time,
  MAX(stage_id) OVER (
    PARTITION BY deal_id ORDER BY change_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )                                                                                                                       AS deal_stage_id,
  MAX(user_id) OVER (PARTITION BY deal_id ORDER BY change_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)          AS deal_user_id,
  MAX(deal_created_at) OVER (PARTITION BY deal_id ORDER BY change_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS deal_created_at,
  MAX(deal_lost_reason) OVER (PARTITION BY deal_id ORDER BY change_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS deal_lost_reason
FROM
  marked_deal_changes

WHERE
  change_time > (SELECT MAX(change_time) FROM "postgres"."public_pipedrive_staging"."stg_deal_changes")

ORDER BY change_time DESC