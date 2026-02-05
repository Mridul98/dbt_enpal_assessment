
      update "postgres"."pipedrive_snapshots"."deal_stages_snapshot"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "deal_stages_snapshot__dbt_tmp152450947756" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "postgres"."pipedrive_snapshots"."deal_stages_snapshot".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and "postgres"."pipedrive_snapshots"."deal_stages_snapshot".dbt_valid_to is null;
      


    insert into "postgres"."pipedrive_snapshots"."deal_stages_snapshot" ("stage_id", "stage_name", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."stage_id",DBT_INTERNAL_SOURCE."stage_name",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "deal_stages_snapshot__dbt_tmp152450947756" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  