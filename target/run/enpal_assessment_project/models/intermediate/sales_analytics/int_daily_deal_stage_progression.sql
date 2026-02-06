
      
  
    

  create  table "postgres"."public_sales_analytics_intermediate"."int_daily_deal_stage_progression__dbt_tmp"
  
  
    as
  
  (
    
WITH deal_changes AS (
  SELECT
    deal_id,
    change_time,
    stage_id,
    user_id

  FROM "postgres"."public_pipedrive_staging"."stg_deal_changes"
  WHERE stage_id IS NOT NULL
),
distinct_deal_stage_entries AS (
 SELECT
  deal_changes.*,
  stages.deal_stage_name,
  FIRST_VALUE(deal_changes.change_time) OVER (PARTITION BY deal_changes.deal_id, deal_changes.stage_id ORDER BY deal_changes.change_time) AS first_entry_date_for_stage
FROM deal_changes
INNER JOIN "postgres"."public_pipedrive_staging"."stg_deal_stages" AS stages
  ON deal_changes.stage_id = stages.deal_stage_id

)
select 
  distinct
  deal_id,
  stage_id,
  user_id,
  first_entry_date_for_stage,
  deal_stage_name
from distinct_deal_stage_entries
  );
  
  