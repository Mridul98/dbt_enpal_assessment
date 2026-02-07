SELECT
  deal_activity_type_stage AS funnel_step,
  deal_activity_type_name  AS funnel_stage_name
FROM {{ ref('stg_pipedrive_deal_activity_types') }}
WHERE deal_activity_type_stage IS NOT NULL
UNION
SELECT
  deal_stage_id::TEXT AS funnel_step,
  deal_stage_name     AS funnel_stage_name
FROM {{ ref('stg_pipedrive_deal_stages') }}
WHERE deal_stage_id IS NOT NULL
