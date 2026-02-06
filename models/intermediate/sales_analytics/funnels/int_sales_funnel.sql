WITH deal_stages AS (
  SELECT
    deal_stage_progression.deal_id,
    deal_stage_progression.deal_stage_name,
    stages.deal_stage_id::TEXT                        AS funnel_step,
    deal_stage_progression.first_entry_date_for_stage AS first_entry_date
  FROM {{ ref('int_deal_stage_progression') }} AS deal_stage_progression
  INNER JOIN {{ ref('stg_pipedrive_deal_stages') }} AS stages
    ON deal_stage_progression.stage_id = stages.deal_stage_id
),

deal_activities AS (
  SELECT
    deal_id,
    deal_activity_type_name,
    deal_activity_type_stage AS funnel_step,
    deal_activity_due_to     AS first_entry_date
  FROM {{ ref('int_deal_activities') }}
  WHERE is_deal_activity_done = TRUE
),

sales_funnel AS (
  SELECT DISTINCT
    deal_id,
    deal_stage_name AS funnel_stage_name,
    funnel_step,
    first_entry_date
  FROM deal_stages
  UNION ALL
  SELECT
    deal_id,
    deal_activity_type_name AS funnel_stage_name,
    funnel_step,
    first_entry_date
  FROM deal_activities
)

SELECT * FROM sales_funnel
