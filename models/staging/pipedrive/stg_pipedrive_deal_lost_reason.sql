SELECT
  field_key_id    AS lost_reason_id,
  field_key_label AS actual_lost_reason
FROM {{ ref('pipedrive_deal_fields_snapshot') }}
WHERE field_key = 'lost_reason'
and dbt_valid_to IS NULL
