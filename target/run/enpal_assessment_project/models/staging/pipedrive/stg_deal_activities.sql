
      -- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
        
    

    

    merge into "postgres"."public_pipedrive_staging"."stg_deal_activities" as DBT_INTERNAL_DEST
        using "stg_deal_activities__dbt_tmp234933894828" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.deal_activity_id = DBT_INTERNAL_DEST.deal_activity_id
                ) and (
                    DBT_INTERNAL_SOURCE.deal_id = DBT_INTERNAL_DEST.deal_id
                )

    
    when matched then update set
        "deal_activity_id" = DBT_INTERNAL_SOURCE."deal_activity_id","deal_activity_type" = DBT_INTERNAL_SOURCE."deal_activity_type","deal_activity_assigned_to_user" = DBT_INTERNAL_SOURCE."deal_activity_assigned_to_user","deal_id" = DBT_INTERNAL_SOURCE."deal_id","is_deal_activity_done" = DBT_INTERNAL_SOURCE."is_deal_activity_done","deal_activity_due_to" = DBT_INTERNAL_SOURCE."deal_activity_due_to"
    

    when not matched then insert
        ("deal_activity_id", "deal_activity_type", "deal_activity_assigned_to_user", "deal_id", "is_deal_activity_done", "deal_activity_due_to")
    values
        ("deal_activity_id", "deal_activity_type", "deal_activity_assigned_to_user", "deal_id", "is_deal_activity_done", "deal_activity_due_to")


  