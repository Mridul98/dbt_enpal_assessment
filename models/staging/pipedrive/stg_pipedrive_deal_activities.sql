{{
    config(
        materialized = 'incremental',
        unique_key = ['deal_activity_id','deal_id'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['deal_activity_id','deal_id'], 'type': 'btree'}
        ],
    )
}}

SELECT
  activity_id      AS deal_activity_id,
  type             AS deal_activity_type,
  assigned_to_user AS deal_activity_assigned_to_user,
  deal_id,
  done             AS is_deal_activity_done,
  due_to           AS deal_activity_due_to,
  NOW()            AS dbt_staging_last_updated_at
FROM {{ source('postgres_public', 'activity') }}
{% if is_incremental() %}
  WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS existing_activities
    WHERE existing_activities.deal_activity_id = activity_id
      AND existing_activities.deal_id = deal_id
  )
{% endif %}
