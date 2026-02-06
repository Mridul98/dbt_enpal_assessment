{% macro filter_new_or_changed_deals(source_relation, relation_to_join_with, timestamp_column, entity_column) %}
    -- This macro filters the source_relation to include only rows that are related to entities (e.g., deals) that have been affected by changes since the last run of the incremental model. 
    {% if is_incremental() %}

    INNER JOIN (

        SELECT DISTINCT source_relation.{{ entity_column }}
        FROM {{ source_relation }} as source_relation
        LEFT JOIN {{ this }} AS existing_records
            ON source_relation.{{ entity_column }} = existing_records.{{ entity_column }}
        WHERE existing_records.{{ entity_column }} IS NULL  -- filtering in only new entities that are not yet in the target table
            OR source_relation.{{ timestamp_column }} > (SELECT MAX({{ timestamp_column }}) FROM {{ this }}) -- filtering in entities that have changes since the last run based on the timestamp column

    ) affected_entities
    ON affected_entities.{{ entity_column }} = {{ relation_to_join_with }}.{{ entity_column }}


    {% endif %}

{% endmacro %}
