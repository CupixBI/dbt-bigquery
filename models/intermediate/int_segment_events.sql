WITH events AS (
    SELECT * FROM {{ ref('stg_cupixworks__segment_events') }}
),

facilities AS (
    SELECT
        region_facility_id,
        facility_name,
        tenant
    FROM {{ ref('int_facility_details') }}
),

teams AS (
    SELECT
        region_team_id,
        tenant,
        account_manager_email,
        primary_csm_email,
        secondary_csm_email,
        team_name,
        lock_state,
        infosphere_builtin_enabled_at
    FROM {{ ref('stg_tesla__teams') }}
),


filtered AS (
    SELECT * FROM events
    WHERE event_name IS NOT NULL and team_id is not null
),

final AS (
    SELECT
        e.*,
        f.facility_name,
        t.account_manager_email,
        t.primary_csm_email,
        t.secondary_csm_email,
        t.team_name,
        t.lock_state,
        t.infosphere_builtin_enabled_at,
    FROM filtered e
    LEFT JOIN facilities f
        ON e.region_facility_id = f.region_facility_id
    LEFT JOIN teams t
        ON e.region_team_id = t.region_team_id
)

SELECT * FROM final