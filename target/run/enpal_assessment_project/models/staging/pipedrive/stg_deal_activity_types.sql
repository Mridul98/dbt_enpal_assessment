
      
  
    

  create  table "postgres"."public_pipedrive_staging"."stg_deal_activity_types__dbt_tmp"
  
  
    as
  
  (
    

SELECT
  id     AS deal_activity_type_id,
  name   AS deal_activity_type_name,
  active AS is_deal_activity_type_active,
  type   AS deal_activity_type
FROM "postgres"."pipedrive_snapshots"."deal_activity_types_snapshot"
where dbt_valid_to is null
  );
  
  