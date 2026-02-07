{% snapshot pipedrive_deal_activity_types_snapshot %}

{{
    config(
        unique_key='id',
        strategy='check',
        hard_deletes='invalidate',
        check_cols='all',
        post_hook="create index if not exists snap_deal_activity_types_current_idx \
            on {{ this }} (type) \
            where dbt_valid_to is null"
    )
    
}}

SELECT 
    id,
    name,
    active,
    type
FROM {{ source('postgres_public','activity_types') }}

{% endsnapshot %}