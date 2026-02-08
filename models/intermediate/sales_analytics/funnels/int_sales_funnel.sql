{{
    config(
        materialized='incremental',
        unique_key=['deal_id', 'funnel_step', 'first_entry_date'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['funnel_month'], 'type': 'btree'},
            {'columns': ['deal_id', 'funnel_step', 'first_entry_date'], 'type': 'btree'}
        ],
    )
}}
WITH deal_stages AS (
  SELECT
    deal_stage_progression.deal_id,
    deal_stage_progression.deal_stage_name,
    stages.deal_stage_id::TEXT                          AS funnel_step,
    deal_stage_progression.first_entry_date_for_stage   AS first_entry_date
  FROM {{ ref('int_deal_stage_progression') }}          AS deal_stage_progression
  INNER JOIN {{ ref('stg_pipedrive_deal_stages') }}     AS stages
    ON deal_stage_progression.stage_id = stages.deal_stage_id
  {% if is_incremental() %}
    WHERE NOT EXISTS (
      SELECT 1
      FROM {{ this }} AS existing_deal_stages
      WHERE
        existing_deal_stages.deal_id = deal_stage_progression.deal_id
        AND existing_deal_stages.funnel_step = stages.deal_stage_id::TEXT
        AND existing_deal_stages.first_entry_date = deal_stage_progression.first_entry_date_for_stage
    )
  {% endif %}
),

deal_activities AS (
  SELECT
    deal_id,
    deal_activity_id,
    deal_activity_type_name,
    deal_activity_type_stage AS funnel_step,
    deal_activity_due_to     AS first_entry_date
  FROM {{ ref('int_deal_activities') }} deal_activities
  WHERE is_deal_activity_done = TRUE
  {% if is_incremental() %}
    AND NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS existing_deal_activities
    WHERE
        existing_deal_activities.deal_id = deal_activities.deal_id
        AND existing_deal_activities.deal_activity_id = deal_activities.deal_activity_id
      
    )
  {% endif %}

),

sales_funnel AS (
  SELECT
    deal_id,
    NULL                    AS deal_activity_id,
    deal_stage_name         AS funnel_stage_name,
    funnel_step,
    first_entry_date
  FROM deal_stages
  UNION ALL
  SELECT
    deal_id,
    deal_activity_id,
    deal_activity_type_name AS funnel_stage_name,
    funnel_step,
    first_entry_date
  FROM deal_activities
)

SELECT 
    deal_id,
    deal_activity_id,
    funnel_stage_name,
    funnel_step,
    first_entry_date,
    (DATE_TRUNC('MONTH', first_entry_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS funnel_month 
FROM sales_funnel
