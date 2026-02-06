{{
  config(
    materialized = 'incremental',
    unique_key = ['deal_id','change_time'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}
WITH marked_deal_changes AS (
  SELECT
    deal_changes.deal_id,
    deal_changes.change_time,
    deal_changes.changed_field_key,
    deal_changes.new_value,
    CASE
      WHEN deal_changes.changed_field_key = 'stage_id' THEN deal_changes.new_value::INT
    END AS stage_id,
    CASE
      WHEN deal_changes.changed_field_key = 'user_id' THEN deal_changes.new_value::INT
    END AS user_id,
    CASE
      WHEN deal_changes.changed_field_key = 'add_time' THEN deal_changes.new_value::TIMESTAMP
    END AS deal_created_at,
    CASE
      WHEN deal_changes.changed_field_key = 'lost_reason' THEN deal_changes.new_value::INT
    END AS deal_lost_reason

  FROM
    {{ source('postgres_public','deal_changes') }} as deal_changes
 
  {{ filter_new_or_changed_deals(
      source_relation=source('postgres_public','deal_changes'), 
      relation_to_join_with='deal_changes',
      timestamp_column='change_time', entity_column='deal_id') 
  }}
),

grouped_marked_deal_changes AS (
  SELECT
    deal_id,
    change_time,
    stage_id,
    user_id,
    deal_created_at,
    deal_lost_reason,
    SUM(CASE WHEN stage_id IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY deal_id ORDER BY change_time)         AS stage_id_change_count,
    SUM(CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY deal_id ORDER BY change_time)          AS user_id_change_count,
    SUM(CASE WHEN deal_created_at IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY deal_id ORDER BY change_time)  AS deal_created_at_change_count,
    SUM(CASE WHEN deal_lost_reason IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY deal_id ORDER BY change_time) AS deal_lost_reason_change_count
  FROM marked_deal_changes
)

SELECT
  deal_id,
  change_time,
  FIRST_VALUE(stage_id) OVER (PARTITION BY deal_id, stage_id_change_count ORDER BY change_time)                 AS stage_id,
  FIRST_VALUE(user_id) OVER (PARTITION BY deal_id, user_id_change_count ORDER BY change_time)                   AS user_id,
  FIRST_VALUE(deal_created_at) OVER (PARTITION BY deal_id, deal_created_at_change_count ORDER BY change_time)   AS deal_created_at,
  FIRST_VALUE(deal_lost_reason) OVER (PARTITION BY deal_id, deal_lost_reason_change_count ORDER BY change_time) AS deal_lost_reason_id
FROM grouped_marked_deal_changes
