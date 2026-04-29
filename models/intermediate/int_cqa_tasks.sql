WITH capture_traces AS (
    SELECT
        *,
        LEAD(timestamp) OVER (
            PARTITION BY region_editing_id
            ORDER BY timestamp
        ) AS next_event_at
    FROM {{ ref('stg_cupixworks__capture_traces') }}
    WHERE class_name = 'Capture' OR class = 'Capture'
),

editings AS (
    SELECT * FROM {{ ref('stg_tesla__editings') }}
),

-- edit 단일 케이스: user_id+tenant dedup으로 cross-region 에디터 매핑
users AS (
    SELECT * FROM {{ ref('stg_tesla__users') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, tenant ORDER BY region) = 1
),

-- edit 복수 케이스: full_name+tenant dedup
users_by_name AS (
    SELECT * FROM {{ ref('stg_tesla__users') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY full_name, tenant ORDER BY region) = 1
),

-- 가장 최근 리뷰어 1명 per editing
reviewers AS (
    SELECT
        r.region_reviewable_id,
        u.user_email AS reviewer_email
    FROM {{ ref('stg_tesla__reviewers') }} r
    LEFT JOIN {{ ref('stg_tesla__users') }} u ON u.region_user_id = r.region_user_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.region_reviewable_id ORDER BY r.created_at DESC) = 1
),

edit_counts AS (
    SELECT
        region_editing_id,
        COUNT(*) AS cnt
    FROM capture_traces
    WHERE LOWER(stage) = 'editing_editing'
    GROUP BY region_editing_id
),

edit_tasks AS (
    SELECT
        CASE
            WHEN ec.cnt = 1 THEN eu.user_email  -- editings 기반 (신뢰성 높음)
            ELSE un.user_email                   -- capture_trace editor_name 기반 (복수 에디터)
        END AS worker_email,
        ct.timestamp AS start_at,
        ct.next_event_at AS end_at,
        ct.region_editing_id,
        'edit' AS task_type
    FROM capture_traces ct
    LEFT JOIN edit_counts ec ON ec.region_editing_id = ct.region_editing_id
    LEFT JOIN editings e ON e.region_editing_id = ct.region_editing_id
    LEFT JOIN users eu ON eu.user_id = e.editor_id AND eu.tenant = e.tenant
    LEFT JOIN users_by_name un ON TRIM(un.full_name) = TRIM(ct.editor_name) AND un.tenant = ct.tenant
    WHERE LOWER(ct.stage) = 'editing_editing'
),

review_tasks AS (
    SELECT
        r.reviewer_email AS worker_email,
        ct.timestamp AS start_at,
        ct.next_event_at AS end_at,
        ct.region_editing_id,
        'review' AS task_type
    FROM capture_traces ct
    LEFT JOIN reviewers r ON r.region_reviewable_id = ct.region_editing_id
    WHERE LOWER(ct.stage) = 'editing_in_review'
),

final AS (
    SELECT worker_email, start_at, end_at, region_editing_id, task_type FROM edit_tasks
    UNION ALL
    SELECT worker_email, start_at, end_at, region_editing_id, task_type FROM review_tasks
)

SELECT * FROM final
