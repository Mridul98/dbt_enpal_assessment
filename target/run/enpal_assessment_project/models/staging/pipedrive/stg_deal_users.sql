
      
  
    

  create  table "postgres"."public_pipedrive_staging"."stg_deal_users__dbt_tmp"
  
  
    as
  
  (
    

SELECT
  id       AS deal_user_id,
  name     AS deal_user_name,
  email    AS deal_user_email,
  modified AS deal_user_last_modified
FROM "postgres"."public"."users"
  );
  
  