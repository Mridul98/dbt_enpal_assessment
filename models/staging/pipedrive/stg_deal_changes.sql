SELECT
  deal_id,
  change_time       AS deal_field_change_time,
  changed_field_key AS deal_changed_field_key,
  new_value         AS deal_changed_field_new_value
FROM {{ source('postgres_public','deal_changes') }}
