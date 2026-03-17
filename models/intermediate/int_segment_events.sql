WITH events AS (
    SELECT * FROM {{ ref('stg_cupixworks__segment_events') }}
),

facilities AS (
    SELECT 
        region_facility_id,
        facility_name,
        account_manager_email,
        primary_csm_email,
        secondary_csm_email
    FROM {{ ref('int_facility_details') }}
),

filtered AS (
    SELECT * FROM events
    WHERE event_name IS NOT NULL
),

final AS (
    SELECT
        e.*,
        f.facility_name,
        f.account_manager_email,
        f.primary_csm_email,
        f.secondary_csm_email
    FROM filtered e
    LEFT JOIN facilities f
        ON e.region_facility_id = f.region_facility_id
)

SELECT * FROM final