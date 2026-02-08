-- ## What this model does
-- This model figures out **when a deal entered each stage for the first time**.
-- Each deal–stage pair appears only once, using the earliest timestamp found.
-- The result is a clean stage progression table for funnel analysis.

-- ## Input: cleaned deal change timeline (example)
-- | deal_id | change_time           | stage_id |
-- |--------:|----------------------|----------|
-- | 881836  | 2024-04-20 21:32:09  | 1 |
-- | 881836  | 2024-05-02 21:32:09  | 2 |
-- | 881836  | 2024-05-05 21:32:09  | 2 |
-- | 900112  | 2024-06-01 10:15:00  | 1 |

-- ## Output: first entry per deal & stage (example)
-- | deal_id | stage_id | first_entry_date_for_stage | deal_stage_name |
-- |--------:|----------|----------------------------|-----------------|
-- | 881836  | 1 | 2024-04-20 21:32:09 | Lead Generation |
-- | 881836  | 2 | 2024-05-02 21:32:09 | Qualified Lead  |
-- | 900112  | 1 | 2024-06-01 10:15:00 | Lead Generation |
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
    MIN(deal_changes.change_time)              AS first_entry_date_for_stage
  FROM {{ ref("stg_pipedrive_deal_changes") }} AS deal_changes
  WHERE
    deal_changes.stage_id IS NOT NULL
    {% if is_incremental() %}
      AND NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS existing_progression
        WHERE
          existing_progression.deal_id = deal_changes.deal_id
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
INNER JOIN {{ ref("stg_pipedrive_deal_stages") }} AS deal_stage
  ON first_deal_stage_entry.stage_id = deal_stage.deal_stage_id
