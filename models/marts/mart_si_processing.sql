WITH editings AS (
    SELECT * FROM {{ ref('int_editings') }}
    WHERE editing_type = 'siteinsights'
),

-- 3월 16일 이후 분리된 review, parent가 siteinsights인 것만
reviews AS (
    SELECT
        parent_id,
        region,
        tenant,
        TIMESTAMP_DIFF(MAX(updated_at), MAX(created_at), MINUTE) AS review_min,
        max(updated_at) as review_finished_at,
        max(updated_at_kst) as review_finished_at_kst,
        editor_email as reviewer_email,
    FROM {{ ref('int_editings') }}
    WHERE editing_type = 'review' 
      AND review_type = 'sqa_review'
    GROUP BY parent_id, region, tenant, reviewer_email
),

captures AS (
    SELECT 
        record_id,
        MAX(uploading_finished_at) AS uploading_finished_at,
        MAX(reconstruction_finished_at) AS reconstruction_finished_at
    FROM {{ ref('int_capture_processing') }}
    WHERE record_id IS NOT NULL
    GROUP BY record_id
),
sitetracks AS (
    SELECT 
        level_id,
        record_id,
        MAX(sitetrack_finished_at) AS sitetrack_finished_at,
        MAX(sitetrack_started_at) AS sitetrack_started_at,
        MAX(error_code) AS error_code,
    FROM {{ ref('int_sitetracks') }}
    WHERE level_id IS NOT NULL AND record_id IS NOT NULL
    GROUP BY level_id, record_id
),

final AS (
    SELECT
        e.*,
        
        -- CQA duration
        TIMESTAMP_DIFF(c.reconstruction_finished_at, c.uploading_finished_at, MINUTE) AS cqa_duration_min,
        
        -- Sitetrack duration
        TIMESTAMP_DIFF(s.sitetrack_finished_at, s.sitetrack_started_at, MINUTE) AS sitetrack_duration_min,
        
        -- SQA duration

        -- 대기 시간: created_at → assigned_at
        TIMESTAMP_DIFF(e.assigned_at, e.created_at, MINUTE) AS sqa_queue_duration_min,
        
        -- 처리 시간: assigned_at → updated_at (or CURRENT_TIMESTAMP)
        TIMESTAMP_DIFF(
            CASE 
                WHEN e.state IN ('done', 'holding') THEN e.updated_at
                ELSE CURRENT_TIMESTAMP()
            END,
            e.assigned_at,
            MINUTE
        ) AS sqa_processing_duration_min,
        s.error_code,

        r.parent_id IS NOT NULL AS has_review_editing,
        r.reviewer_email,


    FROM editings AS e
    LEFT JOIN captures AS c ON c.record_id = e.record_id
    LEFT JOIN sitetracks AS s
        ON s.level_id = e.level_id 
        AND s.record_id = e.record_id
        AND e.level_id IS NOT NULL
        AND e.record_id IS NOT NULL
    LEFT JOIN reviews AS r ON r.parent_id = e.editing_id
)

SELECT * FROM final