SELECT
  activity_id      AS deal_activity_id,
  type             AS deal_activity_type,
  assigned_to_user AS deal_activity_assigned_to_user,
  deal_id,
  done             AS is_deal_activity_done,
  due_to           AS deal_activity_due_to,
  NOW()            AS dbt_staging_last_updated_at
FROM {{ ref('pipedrive_deal_activities_snapshot') }}
WHERE dbt_valid_to IS NULL
