
      -- back compat for old kwarg name
  
  
        
            
                
                
            
        
    

    

    merge into "postgres"."public_pipedrive_staging"."stg_deal_activity_types" as DBT_INTERNAL_DEST
        using "stg_deal_activity_types__dbt_tmp234934289365" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.deal_activity_type_id = DBT_INTERNAL_DEST.deal_activity_type_id
                )

    
    when matched then update set
        "deal_activity_type_id" = DBT_INTERNAL_SOURCE."deal_activity_type_id","deal_activity_type_name" = DBT_INTERNAL_SOURCE."deal_activity_type_name","is_deal_activity_type_active" = DBT_INTERNAL_SOURCE."is_deal_activity_type_active","deal_activity_type" = DBT_INTERNAL_SOURCE."deal_activity_type"
    

    when not matched then insert
        ("deal_activity_type_id", "deal_activity_type_name", "is_deal_activity_type_active", "deal_activity_type")
    values
        ("deal_activity_type_id", "deal_activity_type_name", "is_deal_activity_type_active", "deal_activity_type")


  