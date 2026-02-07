{% snapshot pipedrive_deal_users_snapshot %}

{{
    config(
        unique_key='id',
        strategy='timestamp',
        updated_at='modified',
        hard_deletes='invalidate',
        post_hook="create index if not exists snap_deal_users_current_idx \
            on {{ this }} (id) \
            where dbt_valid_to is null"
    )
    
}}

SELECT 
    id,
    name,
    email,
    modified
FROM {{ source('postgres_public','users') }}

{% endsnapshot %}