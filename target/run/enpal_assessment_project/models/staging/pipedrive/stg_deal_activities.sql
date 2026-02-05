
      
  
    

  create  table "postgres"."public_pipedrive_staging"."stg_deal_activities__dbt_tmp"
  
  
    as
  
  (
    

SELECT
  activity_id      AS deal_activity_id,
  type             AS deal_activity_type,
  assigned_to_user AS deal_activity_assigned_to_user,
  deal_id,
  done             AS is_deal_activity_done,
  due_to           AS deal_activity_due_to
FROM "postgres"."public"."activity"
  );
  
  