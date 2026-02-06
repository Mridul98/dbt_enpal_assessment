{{
    config(
        materialized='incremental',
        unique_key=['deal_stage_id'],
        indexes=[
            {'columns': ['deal_stage_id'], 'type': 'btree'},
            {'columns': ['deal_stage_name'], 'type': 'btree'}
        ],
        incremental_strategy='merge',
        
    )
}}

WITH new_deal_stages_data AS (
  SELECT
    stage_id   AS deal_stage_id,
    stage_name AS deal_stage_name
  FROM {{ ref('deal_stages_snapshot') }}
  WHERE
    dbt_valid_to IS NULL
    AND stage_name IS NOT NULL
)

SELECT
  new_deal_stages_data.deal_stage_id,
  new_deal_stages_data.deal_stage_name
FROM new_deal_stages_data

{% if is_incremental() %}

  LEFT JOIN {{ this }} AS existing_deal_stages_data
    ON
      new_deal_stages_data.deal_stage_id = existing_deal_stages_data.deal_stage_id
  WHERE existing_deal_stages_data.deal_stage_name IS NULL

{% endif %}
