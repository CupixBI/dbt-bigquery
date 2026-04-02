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
        TIMESTAMP_DIFF(state_updated_at, assigned_at, MINUTE) AS review_min
    FROM {{ ref('int_editings') }}
    WHERE editing_type = 'review' 
      AND review_type = 'sqa_review'
),

captures AS (
    SELECT 
        record_id,
        MAX(uploading_finished_at_kst) AS uploading_finished_at_kst,
        MAX(reconstruction_finished_at_kst) AS reconstruction_finished_at_kst
    FROM {{ ref('int_capture_processing') }}
    WHERE record_id IS NOT NULL
    GROUP BY record_id
),
sitetracks AS (
    SELECT 
        level_id,
        record_id,
        MAX(sitetrack_finished_at_kst) AS sitetrack_finished_at_kst,
        MAX(sitetrack_started_at_kst) AS sitetrack_started_at_kst,
        MAX(error_code) AS error_code,
    FROM {{ ref('int_sitetracks') }}
    WHERE level_id IS NOT NULL AND record_id IS NOT NULL
    GROUP BY level_id, record_id
),

final AS (
    SELECT
        e.*,
        
        -- CQA duration
        TIMESTAMP_DIFF(c.reconstruction_finished_at_kst, c.uploading_finished_at_kst, MINUTE) AS cqa_duration_min,
        
        -- Sitetrack duration
        TIMESTAMP_DIFF(s.sitetrack_finished_at_kst, s.sitetrack_started_at_kst, MINUTE) AS sitetrack_duration_min,
        
        -- SQA duration
        TIMESTAMP_DIFF(
            CASE 
                WHEN e.state IN ('done', 'holding') THEN e.state_updated_at
                ELSE CURRENT_TIMESTAMP()
            END,
            e.created_at,
            MINUTE
        ) AS sqa_duration_min,

        -- 대기 시간: created_at → assigned_at
        TIMESTAMP_DIFF(e.assigned_at, e.created_at, Hour) AS sqa_queue_duration_hr,

        -- 처리 시간: assigned_at → state_updated_at (or CURRENT_TIMESTAMP)
        TIMESTAMP_DIFF(
            CASE 
                WHEN e.state IN ('done', 'holding') THEN e.state_updated_at
                ELSE CURRENT_TIMESTAMP()
            END,
            e.assigned_at,
            Hour
        ) AS sqa_processing_duration_hr,
        s.error_code

    FROM editings AS e
    LEFT JOIN captures AS c ON c.record_id = e.record_id
    LEFT JOIN sitetracks AS s
        ON s.level_id = e.level_id 
        AND s.record_id = e.record_id
        AND e.level_id IS NOT NULL
        AND e.record_id IS NOT NULL
)

SELECT * FROM final