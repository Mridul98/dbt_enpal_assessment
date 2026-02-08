{% snapshot pipedrive_deal_activities_snapshot %}

{{
    config(
        unique_key = ['activity_id', 'deal_id'],
        strategy='check',
        check_cols='all',
        hard_deletes='invalidate',
        post_hook="create index if not exists snap_deal_activities_current_idx \
            on {{ this }} (activity_id, deal_id) \
            where dbt_valid_to is null"
    )
}}
SELECT 
    activity_id,
    type,
    assigned_to_user,
    deal_id,
    done,
    due_to
FROM {{ source('postgres_public','activity') }}
{% endsnapshot %}