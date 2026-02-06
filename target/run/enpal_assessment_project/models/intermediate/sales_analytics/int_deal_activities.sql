
  
    

  create  table "postgres"."public_sales_analytics_intermediate"."int_deal_activities__dbt_tmp"
  
  
    as
  
  (
    SELECT 
    deal_activities.deal_activity_id,
    deal_activities.deal_activity_type,
    deal_activities.deal_activity_assigned_to_user,
    deal_activities.deal_id,
    deal_activities.is_deal_activity_done,
    deal_activities.deal_activity_due_to,
    activity_types.deal_activity_type_id,
    activity_types.deal_activity_type_name,
    activity_types.is_deal_activity_type_active,
    activity_types.deal_activity_type_stage

FROM "postgres"."public_pipedrive_staging"."stg_deal_activities" deal_activities
INNER JOIN "postgres"."public_pipedrive_staging"."stg_deal_activity_types" activity_types
    ON deal_activities.deal_activity_type = activity_types.deal_activity_type
WHERE activity_types.is_deal_activity_type_active = TRUE
  );
  