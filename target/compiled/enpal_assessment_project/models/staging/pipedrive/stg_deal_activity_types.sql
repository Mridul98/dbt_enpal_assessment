

SELECT
  id     AS deal_activity_type_id,
  name   AS deal_activity_type_name,
  active AS is_deal_activity_type_active,
  type   AS deal_activity_type
FROM "postgres"."pipedrive_snapshots"."deal_activity_types_snapshot"
WHERE dbt_valid_to IS NULL