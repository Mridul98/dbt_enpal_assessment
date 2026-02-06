with deal_stages as (
    select
        deal_stage_progression.deal_id,
        deal_stage_progression.deal_stage_name,
        stages.deal_stage_id::text as funnel_step,
        deal_stage_progression.first_entry_date_for_stage as first_entry_date
    from "postgres"."public_sales_analytics_intermediate"."int_deal_stage_progression" deal_stage_progression
    inner join "postgres"."public_pipedrive_staging"."stg_deal_stages" as stages
        on deal_stage_progression.stage_id = stages.deal_stage_id
),
deal_activities as (
    select
        deal_id,
        deal_activity_type_name,
        deal_activity_type_stage as funnel_step,
        deal_activity_due_to as first_entry_date
    from "postgres"."public_sales_analytics_intermediate"."int_deal_activities"
    where is_deal_activity_done = True
),
sales_funnel as (
    select distinct
        deal_id,
        deal_stage_name as funnel_stage_name,
        funnel_step,
        first_entry_date
    from deal_stages
    union all
    select
        deal_id,
        deal_activity_type_name as funnel_stage_name,
        funnel_step,
        first_entry_date
    from deal_activities
)
select * from sales_funnel