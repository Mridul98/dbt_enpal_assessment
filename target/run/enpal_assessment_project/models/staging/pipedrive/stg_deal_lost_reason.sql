
      -- back compat for old kwarg name
  
  
        
            
                
                
            
        
    

    

    merge into "postgres"."public_pipedrive_staging"."stg_deal_lost_reason" as DBT_INTERNAL_DEST
        using "stg_deal_lost_reason__dbt_tmp234934018936" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.lost_reason_id = DBT_INTERNAL_DEST.lost_reason_id
                )

    
    when matched then update set
        "lost_reason_id" = DBT_INTERNAL_SOURCE."lost_reason_id","actual_lost_reason" = DBT_INTERNAL_SOURCE."actual_lost_reason"
    

    when not matched then insert
        ("lost_reason_id", "actual_lost_reason")
    values
        ("lost_reason_id", "actual_lost_reason")


  