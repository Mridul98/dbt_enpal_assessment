{% snapshot pipedrive_deal_fields_snapshot %}

{{
    config(
        unique_key=['field_id','field_key_id'],
        strategy='check',
        hard_deletes='invalidate',
        check_cols=['field_id', 'field_key', 'field_key_id','field_name','field_key_label'],
    )
}}

SELECT
    id                  AS field_id,
    field_key           AS field_key,
    name                AS field_name,
    (array_elems ->> 'id')::INT     AS field_key_id,
    (array_elems ->> 'label')::TEXT AS field_key_label
FROM {{ source('postgres_public','fields') }},
LATERAL JSONB_ARRAY_ELEMENTS(field_value_options) AS array_elems

{% endsnapshot %}