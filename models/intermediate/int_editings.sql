WITH editings AS (
    SELECT * FROM {{ ref('stg_tesla__editings') }}
),

users AS (
    SELECT * EXCEPT(rn) FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY user_id, region, tenant ORDER BY created_at DESC) AS rn
        FROM {{ ref('stg_tesla__users') }}
    )
    WHERE rn = 1
),

captures AS (
    SELECT 
        record_id,
        region,
        tenant,
        MAX(region_capture_id) AS region_capture_id,
        MAX(capture_type) AS capture_type,
        MAX(created_at) AS created_at,
        MAX(uploading_finished_at) AS uploading_finished_at
    FROM {{ ref('int_capture_processing') }}
    GROUP BY record_id, region, tenant
),

facilities AS (
    SELECT
        facility_id,
        facility_name,
        team_id,
        team_name,
        region,
        tenant,
    FROM {{ ref('int_facility_details') }}
),

final AS (
    SELECT
        e.region,
        e.editing_id,
        e.region_editing_id,
        e.stat_total_entities,
        e.created_at,
        e.tenant,
        TIMESTAMP_ADD(e.created_at, INTERVAL 9 HOUR) AS created_at_kst,
        e.estimated_finish_at,
        TIMESTAMP_ADD(e.estimated_finish_at, INTERVAL 9 HOUR) AS estimated_finish_at_kst,
        e.state_updated_at,
        TIMESTAMP_ADD(e.state_updated_at, INTERVAL 9 HOUR) AS state_updated_at_kst,
        e.updated_at,
        TIMESTAMP_ADD(e.updated_at, INTERVAL 9 HOUR) AS updated_at_kst,
        e.assigned_at,
        TIMESTAMP_ADD(e.assigned_at, INTERVAL 9 HOUR) AS assigned_at_kst,
        e.state,
        e.editor_id,
        e.editing_type,
        e.level_id,
        e.record_id,
        e.parent_id,
        e.facility_id AS project_id,
        f.facility_name AS project_name,
        f.team_id,
        f.team_name,
        CASE
            WHEN e.editing_type != 'review' THEN NULL
            WHEN p.editing_type = 'siteinsights' THEN 'sqa_review'
            WHEN p.editing_type IS NULL THEN 'unknown'
            ELSE 'cqa_review'
        END AS review_type,
        c.region_capture_id,
        c.capture_type,
        c.created_at AS capture_created_at,
        c.uploading_finished_at AS capture_uploading_finished_at,
        u.user_email AS editor_email,
        u.editor_level AS editor_level,
        CONCAT(u.firstname, ' ', u.lastname) AS editor_name
    FROM editings AS e
    LEFT JOIN editings AS p ON p.editing_id = e.parent_id AND p.region = e.region  AND p.tenant = e.tenant  
    LEFT JOIN captures AS c ON c.record_id = e.record_id AND c.tenant = e.tenant AND c.region = e.region
    LEFT JOIN users AS u 
        ON u.user_id = e.editor_id
        AND u.region = e.region
        AND u.tenant = e.tenant
    LEFT JOIN facilities AS f
        ON CAST(f.facility_id AS STRING) = e.facility_id
        AND f.region = e.region
        AND f.tenant = e.tenant
        
)

SELECT * FROM final