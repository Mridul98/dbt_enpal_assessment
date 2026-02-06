{{
    config(
        materialized='incremental',
        unique_key='deal_field_key',
        incremental_strategy='merge'
    )
}}

SELECT
  id                  AS deal_field_id,
  field_key           AS deal_field_key,
  name                AS deal_field_name,
  field_value_options AS deal_field_value_options
FROM {{ ref('deal_fields_snapshot') }}
WHERE dbt_valid_to IS NULL
