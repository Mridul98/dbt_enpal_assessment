
      -- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into "postgres"."public_pipedrive_staging"."stg_deal_users" as DBT_INTERNAL_DEST
        using "stg_deal_users__dbt_tmp234934050116" as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.deal_user_id = DBT_INTERNAL_DEST.deal_user_id))

    
    when matched then update set
        "deal_user_id" = DBT_INTERNAL_SOURCE."deal_user_id","deal_user_name" = DBT_INTERNAL_SOURCE."deal_user_name","deal_user_email" = DBT_INTERNAL_SOURCE."deal_user_email","deal_user_last_modified" = DBT_INTERNAL_SOURCE."deal_user_last_modified"
    

    when not matched then insert
        ("deal_user_id", "deal_user_name", "deal_user_email", "deal_user_last_modified")
    values
        ("deal_user_id", "deal_user_name", "deal_user_email", "deal_user_last_modified")


  