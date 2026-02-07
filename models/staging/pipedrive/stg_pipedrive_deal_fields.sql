SELECT
  field_id        AS deal_field_id,
  field_key       AS deal_field_key,
  field_name      AS deal_field_name,
  field_key_label AS deal_field_value_options
FROM {{ ref('pipedrive_deal_fields_snapshot') }}
WHERE dbt_valid_to IS NULL
