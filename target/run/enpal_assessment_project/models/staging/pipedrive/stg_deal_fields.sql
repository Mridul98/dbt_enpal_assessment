
      -- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into "postgres"."public_pipedrive_staging"."stg_deal_fields" as DBT_INTERNAL_DEST
        using "stg_deal_fields__dbt_tmp234934315757" as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.deal_field_key = DBT_INTERNAL_DEST.deal_field_key))

    
    when matched then update set
        "deal_field_id" = DBT_INTERNAL_SOURCE."deal_field_id","deal_field_key" = DBT_INTERNAL_SOURCE."deal_field_key","deal_field_name" = DBT_INTERNAL_SOURCE."deal_field_name","deal_field_value_options" = DBT_INTERNAL_SOURCE."deal_field_value_options"
    

    when not matched then insert
        ("deal_field_id", "deal_field_key", "deal_field_name", "deal_field_value_options")
    values
        ("deal_field_id", "deal_field_key", "deal_field_name", "deal_field_value_options")


  