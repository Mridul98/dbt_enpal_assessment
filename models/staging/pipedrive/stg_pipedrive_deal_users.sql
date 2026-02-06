{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='deal_user_id',
        indexes=[
            {'columns': ['deal_user_id'], 'type': 'btree'}
        ],
    )
}}

SELECT
  id       AS deal_user_id,
  name     AS deal_user_name,
  email    AS deal_user_email,
  modified AS deal_user_last_modified
FROM {{ source('postgres_public','users') }}
