{{
    config(
        materialized = 'incremental',
        unique_key = ['deal_activity_id','deal_id'],
        incremental_strategy='merge'
    )
}}

SELECT
  activity_id      AS deal_activity_id,
  type             AS deal_activity_type,
  assigned_to_user AS deal_activity_assigned_to_user,
  deal_id,
  done             AS is_deal_activity_done,
  due_to           AS deal_activity_due_to
FROM {{ source('postgres_public','activity') }}
