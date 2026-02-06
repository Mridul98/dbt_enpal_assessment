SELECT
  deal_activities.deal_activity_id,
  deal_activities.deal_activity_type,
  deal_activities.deal_activity_assigned_to_user,
  deal_activities.deal_id,
  deal_activities.is_deal_activity_done,
  deal_activities.deal_activity_due_to,
  activity_types.deal_activity_type_id,
  activity_types.deal_activity_type_name,
  activity_types.is_deal_activity_type_active,
  activity_types.deal_activity_type_stage

FROM {{ ref('stg_pipedrive_deal_activities') }} AS deal_activities
INNER JOIN {{ ref('stg_pipedrive_deal_activity_types') }} AS activity_types
  ON deal_activities.deal_activity_type = activity_types.deal_activity_type
WHERE activity_types.is_deal_activity_type_active = TRUE
