
      -- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
        
    

    

    merge into "postgres"."public_pipedrive_staging"."stg_deal_changes" as DBT_INTERNAL_DEST
        using "stg_deal_changes__dbt_tmp032554195944" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.deal_id = DBT_INTERNAL_DEST.deal_id
                ) and (
                    DBT_INTERNAL_SOURCE.change_time = DBT_INTERNAL_DEST.change_time
                )

    
    when matched then update set
        "deal_id" = DBT_INTERNAL_SOURCE."deal_id","change_time" = DBT_INTERNAL_SOURCE."change_time","stage_id" = DBT_INTERNAL_SOURCE."stage_id","user_id" = DBT_INTERNAL_SOURCE."user_id","deal_created_at" = DBT_INTERNAL_SOURCE."deal_created_at","deal_lost_reason" = DBT_INTERNAL_SOURCE."deal_lost_reason"
    

    when not matched then insert
        ("deal_id", "change_time", "stage_id", "user_id", "deal_created_at", "deal_lost_reason")
    values
        ("deal_id", "change_time", "stage_id", "user_id", "deal_created_at", "deal_lost_reason")


  