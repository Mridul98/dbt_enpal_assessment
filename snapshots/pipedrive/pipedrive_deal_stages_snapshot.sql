{% snapshot pipedrive_deal_stages_snapshot %}

{{
    config(
        unique_key='stage_id',
        strategy='check',
        check_cols='all',
        hard_deletes='invalidate',
        post_hook="create index if not exists snap_deal_stages_current_idx \
            on {{ this }} (stage_id) \
            where dbt_valid_to is null"
    )
    
}}

SELECT 
   stage_id,
   stage_name
FROM {{ source('postgres_public','stages') }}

{% endsnapshot %}