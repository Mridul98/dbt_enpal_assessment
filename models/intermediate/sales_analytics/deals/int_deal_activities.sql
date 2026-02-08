{{ 
  config(
    materialized='incremental',
    unique_key=['deal_activity_id','deal_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    indexes=[
        {'columns': ['deal_activity_id','deal_id'], 'type': 'btree'}
    ],
  ) 
}}

{% if is_incremental() %}
  {% set get_last_updated_at = run_query('select coalesce(max(dbt_staging_last_updated_at), \'1900-01-01 00:00:00\') from ' + this.schema + '.' + this.name) %}
  {% set last_updated_at = get_last_updated_at.rows[0][0] %}
{% endif %}


WITH deal_activities AS (
  SELECT
    deal_activities.deal_activity_id,
    deal_activities.deal_activity_type,
    deal_activities.deal_activity_assigned_to_user,
    deal_activities.deal_id,
    deal_activities.is_deal_activity_done,
    deal_activities.deal_activity_due_to,
    deal_activities.dbt_staging_last_updated_at,
    activity_types.deal_activity_type_id,
    activity_types.deal_activity_type_name,
    activity_types.is_deal_activity_type_active,
    activity_types.deal_activity_type_stage
  FROM {{ ref('stg_pipedrive_deal_activities') }}           AS deal_activities
  INNER JOIN {{ ref('stg_pipedrive_deal_activity_types') }} AS activity_types
    ON deal_activities.deal_activity_type = activity_types.deal_activity_type
  {% if is_incremental() %}
    WHERE deal_activities.dbt_staging_last_updated_at > '{{ last_updated_at }}'
  {% endif %}
)

SELECT
  deal_activity_id,
  deal_activity_type,
  deal_activity_assigned_to_user,
  deal_id,
  is_deal_activity_done,
  deal_activity_due_to,
  dbt_staging_last_updated_at,
  deal_activity_type_id,
  deal_activity_type_name,
  is_deal_activity_type_active,
  deal_activity_type_stage
FROM deal_activities
