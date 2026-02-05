{{
    config(
        materialized='incremental',
        unique_key = ['deal_activity_type_id'],
        incremental_strategy='merge' 
    )
}}

SELECT
  id     AS deal_activity_type_id,
  name   AS deal_activity_type_name,
  active AS is_deal_activity_type_active,
  type   AS deal_activity_type
FROM {{ ref('deal_activity_types_snapshot') }}
where dbt_valid_to is null
