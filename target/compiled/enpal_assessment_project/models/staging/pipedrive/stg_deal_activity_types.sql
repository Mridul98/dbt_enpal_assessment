

SELECT
  id                              AS deal_activity_type_id,
  name                            AS deal_activity_type_name,
  case
    when name = 'Sales Call 1' then '2.1'
    when name = 'Sales Call 2' then '3.1'
    else NULL
  end                             AS deal_activity_type_stage,
  COALESCE(active = 'Yes', FALSE) AS is_deal_activity_type_active,
  type                            AS deal_activity_type
FROM "postgres"."pipedrive_snapshots"."deal_activity_types_snapshot"
WHERE dbt_valid_to IS NULL