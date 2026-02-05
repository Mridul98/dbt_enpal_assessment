
  
    

  create  table "postgres"."public_pipedrive_staging"."stg_deal_changes__dbt_tmp"
  
  
    as
  
  (
    SELECT
  deal_id,
  change_time       AS deal_field_change_time,
  changed_field_key AS deal_changed_field_key,
  new_value         AS deal_changed_field_new_value
FROM "postgres"."public"."deal_changes"
  );
  