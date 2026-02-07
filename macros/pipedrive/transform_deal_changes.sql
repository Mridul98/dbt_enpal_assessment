{% macro transform_deal_changes(table_ref) %}
-- this macro transforms the pipedrive deal changes table by extracting relevant fields
-- add more fields as necessary in the future here
-- input: table_ref - the reference to the deal changes table
    {{ table_ref }}.deal_id,
    {{ table_ref }}.change_time,
    {{ table_ref }}.changed_field_key,
    {{ table_ref }}.new_value,
    CASE
        WHEN {{ table_ref }}.changed_field_key = 'stage_id' THEN {{ table_ref }}.new_value::INT
    END AS stage_id,
    CASE
        WHEN {{ table_ref }}.changed_field_key = 'user_id' THEN {{ table_ref }}.new_value::INT
    END AS user_id,
    CASE
        WHEN {{ table_ref }}.changed_field_key = 'add_time' THEN {{ table_ref }}.new_value::TIMESTAMP
    END AS deal_created_at,
    CASE
        WHEN {{ table_ref }}.changed_field_key = 'lost_reason' THEN {{ table_ref }}.new_value::INT
    END AS deal_lost_reason
{% endmacro %}