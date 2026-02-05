
      -- back compat for old kwarg name
  
  
        
            
                
                
            
        
    

    

    merge into "postgres"."public_pipedrive_staging"."stg_deal_stages" as DBT_INTERNAL_DEST
        using "stg_deal_stages__dbt_tmp234934341788" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.deal_stage_id = DBT_INTERNAL_DEST.deal_stage_id
                )

    
    when matched then update set
        "deal_stage_id" = DBT_INTERNAL_SOURCE."deal_stage_id","deal_stage_name" = DBT_INTERNAL_SOURCE."deal_stage_name"
    

    when not matched then insert
        ("deal_stage_id", "deal_stage_name")
    values
        ("deal_stage_id", "deal_stage_name")


  