
      -- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into "postgres"."public"."int_monthly_deal_stage_progression" as DBT_INTERNAL_DEST
        using "int_monthly_deal_stage_progression__dbt_tmp032554598826" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.deal_id = DBT_INTERNAL_DEST.deal_id
                ) and (
                    DBT_INTERNAL_SOURCE.change_time = DBT_INTERNAL_DEST.change_time
                ) and (
                    DBT_INTERNAL_SOURCE.stage_id = DBT_INTERNAL_DEST.stage_id
                )

    
    when matched then update set
        "deal_id" = DBT_INTERNAL_SOURCE."deal_id","change_time" = DBT_INTERNAL_SOURCE."change_time","change_month" = DBT_INTERNAL_SOURCE."change_month","change_year" = DBT_INTERNAL_SOURCE."change_year","stage_id" = DBT_INTERNAL_SOURCE."stage_id","deal_stage_name" = DBT_INTERNAL_SOURCE."deal_stage_name"
    

    when not matched then insert
        ("deal_id", "change_time", "change_month", "change_year", "stage_id", "deal_stage_name")
    values
        ("deal_id", "change_time", "change_month", "change_year", "stage_id", "deal_stage_name")


  