{{
    config(
        materialized = 'incremental',
        unique_key = ['deal_id', 'user_id', 'stage_id'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
     )
}}

WITH deal_changes AS (
  SELECT
    deal_id,
    change_time,
    stage_id,
    user_id

  FROM {{ ref('stg_pipedrive_deal_changes') }}
  WHERE stage_id IS NOT NULL
),

distinct_deal_stage_entries AS (
  SELECT
    deal_changes.*,
    stages.deal_stage_name,
    FIRST_VALUE(deal_changes.change_time) OVER (PARTITION BY deal_changes.deal_id, deal_changes.stage_id ORDER BY deal_changes.change_time) AS first_entry_date_for_stage
  FROM deal_changes
  INNER JOIN {{ ref('stg_pipedrive_deal_stages') }} AS stages
    ON deal_changes.stage_id = stages.deal_stage_id
)

SELECT DISTINCT
  distinct_deal_stage_entries.deal_id,
  distinct_deal_stage_entries.stage_id,
  distinct_deal_stage_entries.user_id,
  distinct_deal_stage_entries.first_entry_date_for_stage,
  distinct_deal_stage_entries.deal_stage_name
FROM distinct_deal_stage_entries

{% if is_incremental() %}
  LEFT JOIN {{ this }} AS existing_deal_stage_progression
    ON
      distinct_deal_stage_entries.deal_id = existing_deal_stage_progression.deal_id
      AND distinct_deal_stage_entries.stage_id = existing_deal_stage_progression.stage_id
      AND distinct_deal_stage_entries.user_id = existing_deal_stage_progression.user_id
  WHERE
    existing_deal_stage_progression.deal_id IS NULL
    AND existing_deal_stage_progression.stage_id IS NULL
    AND existing_deal_stage_progression.user_id IS NULL
{% endif %}
