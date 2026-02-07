{{
    config(
        materialized = 'incremental',
        unique_key = ['deal_id', 'stage_id'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['deal_id', 'stage_id'], 'type': 'btree'}
        ]
     )
}}
WITH first_deal_stage_entry AS (
  SELECT
    deal_changes.deal_id,
    deal_changes.stage_id,
    MIN(deal_changes.change_time) AS first_entry_date_for_stage
  FROM {{ ref("stg_pipedrive_deal_changes") }} deal_changes
  WHERE deal_changes.stage_id IS NOT NULL
  {% if is_incremental() %}
    AND NOT EXISTS (
      SELECT 1
      FROM {{ this }} existing_progression
      WHERE
        existing_progression.deal_id  = deal_changes.deal_id
        AND existing_progression.stage_id = deal_changes.stage_id
    )
  {% endif %}
  GROUP BY
    deal_changes.deal_id,
    deal_changes.stage_id
)

SELECT
  first_deal_stage_entry.deal_id,
  first_deal_stage_entry.stage_id,
  first_deal_stage_entry.first_entry_date_for_stage,
  deal_stage.deal_stage_name
FROM first_deal_stage_entry
JOIN {{ ref("stg_pipedrive_deal_stages") }} deal_stage 
  ON first_deal_stage_entry.stage_id = deal_stage.deal_stage_id
