
SELECT
  (array_elems ->> 'id')::INT     AS lost_reason_id,
  (array_elems ->> 'label')::TEXT AS actual_lost_reason
FROM public.fields,
  LATERAL JSONB_ARRAY_ELEMENTS(field_value_options) AS array_elems
WHERE field_key = 'lost_reason'