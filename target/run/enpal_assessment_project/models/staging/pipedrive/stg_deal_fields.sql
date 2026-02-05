
      
  
    

  create  table "postgres"."public_pipedrive_staging"."stg_deal_fields__dbt_tmp"
  
  
    as
  
  (
    

SELECT
  id                  AS deal_field_id,
  field_key           AS deal_field_key,
  name                AS deal_field_name,
  field_value_options AS deal_field_value_options
FROM "postgres"."pipedrive_snapshots"."deal_fields_snapshot"
WHERE dbt_valid_to IS NULL
  );
  
  