
      -- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into "postgres"."public_sales_analytics_intermediate"."int_monthly_deal_stage_progression" as DBT_INTERNAL_DEST
        using "int_monthly_deal_stage_progression__dbt_tmp190944020891" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.deal_id = DBT_INTERNAL_DEST.deal_id
                ) and (
                    DBT_INTERNAL_SOURCE.change_time = DBT_INTERNAL_DEST.change_time
                ) and (
                    DBT_INTERNAL_SOURCE.stage_id = DBT_INTERNAL_DEST.stage_id
                )

    
    when matched then update set
        "deal_id" = DBT_INTERNAL_SOURCE."deal_id","change_time" = DBT_INTERNAL_SOURCE."change_time","stage_id" = DBT_INTERNAL_SOURCE."stage_id","deal_lost_reason_id" = DBT_INTERNAL_SOURCE."deal_lost_reason_id","deal_stage_name" = DBT_INTERNAL_SOURCE."deal_stage_name","actual_lost_reason" = DBT_INTERNAL_SOURCE."actual_lost_reason","first_entry_date_for_stage" = DBT_INTERNAL_SOURCE."first_entry_date_for_stage"
    

    when not matched then insert
        ("deal_id", "change_time", "stage_id", "deal_lost_reason_id", "deal_stage_name", "actual_lost_reason", "first_entry_date_for_stage")
    values
        ("deal_id", "change_time", "stage_id", "deal_lost_reason_id", "deal_stage_name", "actual_lost_reason", "first_entry_date_for_stage")


  