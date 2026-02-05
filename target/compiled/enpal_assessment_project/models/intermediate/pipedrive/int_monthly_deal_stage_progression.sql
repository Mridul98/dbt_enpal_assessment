
with deal_changes as (
    select
        deal_id,
        change_time,
        (date_trunc('month', change_time) + interval '1 month' - interval '1 day')::DATE AS change_month,
        (extract('year' from change_time))::int AS change_year,
        stage_id
    from "postgres"."public_pipedrive_staging"."stg_deal_changes"
    where stage_id is not null
)
select 
    deal_changes.*,
    stages.deal_stage_name
from deal_changes
inner join "postgres"."public_pipedrive_staging"."stg_deal_stages" as stages
    on deal_changes.stage_id = stages.deal_stage_id