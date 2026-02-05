
      update "postgres"."pipedrive_snapshots"."stg_deal_activity_types_snapshot"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "stg_deal_activity_types_snapshot__dbt_tmp144718969973" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "postgres"."pipedrive_snapshots"."stg_deal_activity_types_snapshot".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and "postgres"."pipedrive_snapshots"."stg_deal_activity_types_snapshot".dbt_valid_to is null;
      


    insert into "postgres"."pipedrive_snapshots"."stg_deal_activity_types_snapshot" ("id", "name", "active", "type", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."id",DBT_INTERNAL_SOURCE."name",DBT_INTERNAL_SOURCE."active",DBT_INTERNAL_SOURCE."type",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "stg_deal_activity_types_snapshot__dbt_tmp144718969973" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  