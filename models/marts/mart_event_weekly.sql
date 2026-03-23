WITH events AS (
    SELECT * FROM {{ ref('int_segment_events') }}
),

final AS (
    SELECT
        DATE_TRUNC(event_timestamp, WEEK) AS week,
        region_user_id,
        user_email,
        region_team_id,
        team_name,
        event_name,
        facility_name,
        workspace_name,
        account_manager_email,
        primary_csm_email,
        secondary_csm_email,
        lock_state as team_lock_state,
        infosphere_builtin_enabled_at,
        COUNT(*) AS event_count
    FROM events
    WHERE region_team_id IS NOT NULL and user_email not like '%cupix%'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
)

SELECT * FROM final