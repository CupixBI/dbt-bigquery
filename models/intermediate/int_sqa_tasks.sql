WITH capture_traces AS (
    SELECT * FROM {{ ref('stg_cupixworks__capture_traces') }}
    WHERE class_name = 'Editing' OR class = 'Editing'
),

editings AS (
    SELECT * FROM {{ ref('stg_tesla__editings') }}
    WHERE editing_type = 'siteinsights'
),

users AS (
    SELECT * EXCEPT(rn) FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY user_id, region, tenant ORDER BY created_at DESC) AS rn
        FROM {{ ref('stg_tesla__users') }}
    )
    WHERE rn = 1
),

reviewers AS (
    SELECT
        r.region_reviewable_id,
        r.user_id AS reviewer_id,
        u.user_email AS reviewer_email
    FROM {{ ref('stg_tesla__reviewers') }} r
    LEFT JOIN users u ON u.region_user_id = r.region_user_id
    WHERE r.reviewable_type = 'Editing'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.region_reviewable_id ORDER BY r.created_at DESC) = 1
),

facilities AS (
    SELECT region_facility_id, facility_name
    FROM {{ ref('stg_tesla__facilities') }}
),

teams AS (
    SELECT region_team_id, team_name
    FROM {{ ref('stg_tesla__teams') }}
),

base_data AS (
    SELECT
        LOWER(ct.stage) AS event_name,
        ct.timestamp AS time,
        ct.region_editing_id,
        e.editing_id,
        e.region,
        e.region_simplify,
        e.tenant,
        e.editing_type,
        e.state AS task_state,
        e.editor_id,
        e.region_facility_id,
        e.region_team_id,
        eu.user_email AS editor_email,
        r.reviewer_id,
        r.reviewer_email
    FROM capture_traces ct
    INNER JOIN editings e ON e.region_editing_id = ct.region_editing_id
    LEFT JOIN users eu
        ON eu.user_id = e.editor_id
        AND eu.region = e.region
        AND eu.tenant = e.tenant
    LEFT JOIN reviewers r ON r.region_reviewable_id = ct.region_editing_id
    WHERE LOWER(ct.stage) IN (
        'editing_editing',
        'editing_waiting_for_review',
        'editing_in_review',
        'editing_done'
    )
),

event_pivot AS (
    SELECT
        region_editing_id,
        editing_id,
        region,
        region_simplify,
        tenant,
        editing_type,
        task_state,
        editor_id,
        region_facility_id,
        region_team_id,
        MAX(editor_email)   AS editor_email,
        MAX(reviewer_id)    AS reviewer_id,
        MAX(reviewer_email) AS reviewer_email,
        MIN(CASE WHEN event_name = 'editing_editing'            THEN time END) AS editing_start_at,
        MIN(CASE WHEN event_name = 'editing_waiting_for_review' THEN time END) AS editing_end_at,
        MIN(CASE WHEN event_name = 'editing_in_review'          THEN time END) AS review_start_at,
        MAX(CASE WHEN event_name = 'editing_done'               THEN time END) AS done_at
    FROM base_data
    GROUP BY
        region_editing_id, editing_id, region, region_simplify,
        tenant, editing_type, task_state, editor_id,
        region_facility_id, region_team_id
),

final AS (
    SELECT
        DATE(done_at) AS date,
        region_simplify AS region,
        region_editing_id,
        editing_id,
        editor_email,
        editor_id,
        reviewer_email,
        reviewer_id,
        editing_type AS task_type,
        task_state,
        tenant,
        ep.region_facility_id,
        f.facility_name,
        ep.region_team_id,
        t.team_name,
        editing_start_at,
        editing_end_at,
        review_start_at,
        done_at,
        GREATEST(0, CASE
            WHEN editing_start_at IS NOT NULL AND editing_end_at IS NOT NULL
            THEN TIMESTAMP_DIFF(editing_end_at, editing_start_at, SECOND)
            ELSE 0
        END) AS editing_duration_sec,
        GREATEST(0, CASE
            WHEN review_start_at IS NOT NULL AND done_at IS NOT NULL
            THEN TIMESTAMP_DIFF(done_at, review_start_at, SECOND)
            ELSE 0
        END) AS review_duration_sec,
        GREATEST(0, CASE
            WHEN editing_start_at IS NOT NULL AND editing_end_at IS NOT NULL
            THEN TIMESTAMP_DIFF(editing_end_at, editing_start_at, SECOND)
            ELSE 0
        END) + GREATEST(0, CASE
            WHEN review_start_at IS NOT NULL AND done_at IS NOT NULL
            THEN TIMESTAMP_DIFF(done_at, review_start_at, SECOND)
            ELSE 0
        END) AS total_duration_sec
    FROM event_pivot ep
    LEFT JOIN facilities f ON f.region_facility_id = ep.region_facility_id
    LEFT JOIN teams t ON t.region_team_id = ep.region_team_id
    WHERE ep.done_at IS NOT NULL
)

SELECT * FROM final
