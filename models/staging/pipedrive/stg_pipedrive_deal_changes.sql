-- This model turns a raw “one-change-per-row” log into a readable deal timeline.
-- Each row in the output represents the **first time a deal got a new value**.
-- Repeated values are ignored so only meaningful changes remain.

-- ## Input: raw deal change log (example)
-- | deal_id | change_time           | changed_field | new_value |
-- |--------:|----------------------|---------------|-----------|
-- | 881836  | 2024-04-09 21:32:09  | add_time      | 2024-04-09 |
-- | 881836  | 2024-04-12 21:32:09  | user_id       | 1463 |
-- | 881836  | 2024-04-17 21:32:09  | user_id       | 1728 |
-- | 881836  | 2024-04-20 21:32:09  | stage_id      | 1 |
-- | 881836  | 2024-05-02 21:32:09  | stage_id      | 2 |

-- ## Output: clean deal timeline (example)
-- | deal_id | change_time           | stage_id | user_id | deal_created_at |
-- |--------:|----------------------|----------|---------|-----------------|
-- | 881836  | 2024-04-09 21:32:09  | NULL     | NULL    | 2024-04-09 |
-- | 881836  | 2024-04-12 21:32:09  | NULL     | 1463    | 2024-04-09 |
-- | 881836  | 2024-04-17 21:32:09  | NULL     | 1728    | 2024-04-09 |
-- | 881836  | 2024-04-20 21:32:09  | 1        | 1728    | 2024-04-09 |
-- | 881836  | 2024-05-02 21:32:09  | 2        | 1728    | 2024-04-09 |
{{
  config(
    materialized = 'incremental',
    unique_key = ['deal_id','change_time'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook = [
      '''CREATE INDEX IF NOT EXISTS idx_composite_deal_id_stage_id
        ON {{this}} (deal_id, stage_id)
      ''',
      '''CREATE INDEX IF NOT EXISTS idx_partial_deal_id_change_time 
        ON {{this}} (deal_id, change_time) where stage_id IS NOT NULL
      ''',
      '''CREATE INDEX IF NOT EXISTS idx_sort_change_time 
        ON {{this}} (change_time DESC)
      '''
    ]  
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
