{{
  config(
    materialized = 'incremental',
    unique_key = ['deal_id','change_time'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook = '''CREATE INDEX IF NOT EXISTS idx_stg_change_time_desc
      ON {{ this }}(change_time DESC)
      INCLUDE (deal_id);'''
  )
}}

{% if is_incremental() %}

WITH max_existing_change_time AS (
  SELECT MAX(change_time) AS max_change_time
  FROM {{ this }}
),
marked_deal_changes AS (

  -- deals that have changes after the last change_time we have in our table, we want to capture those changes
  SELECT 
   {{ transform_deal_changes('deal_changes') }}
  FROM {{ source('postgres_public','deal_changes') }} deal_changes
  cross join max_existing_change_time
  WHERE change_time > max_existing_change_time.max_change_time

  UNION ALL

  -- deals that are new and don't have any changes yet, but we want to capture them as well
  SELECT 
    {{ transform_deal_changes('new_deals') }}
  FROM {{ source('postgres_public','deal_changes') }} new_deals
  WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }}
    WHERE {{ this }}.deal_id = new_deals.deal_id
  )
),
    
{% else %}
-- For the initial run, we want to capture all changes, so no additional filter is needed
WITH marked_deal_changes AS (
  SELECT
    {{ transform_deal_changes('deal_changes') }}
  FROM
    {{ source('postgres_public','deal_changes') }} as deal_changes
),
{% endif %}


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
