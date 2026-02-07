{{
    config(
        materialized='incremental',
        unique_key = ['deal_activity_type_id'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
  id                              AS deal_activity_type_id,
  name                            AS deal_activity_type_name,
  case
    when name = 'Sales Call 1' then '2.1'
    when name = 'Sales Call 2' then '3.1'
    else NULL
  end                             AS deal_activity_type_stage,
  COALESCE(active = 'Yes', FALSE) AS is_deal_activity_type_active,
  type                            AS deal_activity_type
FROM {{ ref('pipedrive_deal_activity_types_snapshot') }}
WHERE dbt_valid_to IS NULL
