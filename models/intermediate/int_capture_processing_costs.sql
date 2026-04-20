/*
    int_capture_processing_costs.sql
    
    목적: 캡처별 Processing 비용 + Editing 인건비 계산
    
    비용 로직:
      - Processing 비용: video_length × 0.00540839 (캡처별 개별)
      - Editing 인건비: (edit_min + review_min) / 60 × editor unit_price(시급)
        → 같은 task(region_team_id + edit_started_at) 내 캡처 수로 나누기
*/

WITH capture_processing AS (
    SELECT
        region_capture_id,
        tenant,
        video_length,
        uploading_finished_at,
        edit_started_at,
        edit_finished_at,
        review_started_at,
        review_finished_at,
        TIMESTAMP_DIFF(edit_finished_at, edit_started_at, MINUTE)     AS edit_started_to_edit_finished_min,
        TIMESTAMP_DIFF(review_finished_at, review_started_at, MINUTE) AS review_started_to_review_finished_min
    FROM {{ ref('int_capture_processing') }}
),

capture_details AS (
    SELECT
        region_capture_id,
        tenant,
        region_team_id,
        editor_email,
        editor_level,
        editor_work_part
    FROM {{ ref('int_capture_details') }}
),

cqa_editors AS (
    SELECT * FROM {{ ref('stg_finance__cqa_editors') }}
),

joined AS (
    SELECT
        cp.*,
        cd.region_team_id,
        cd.editor_email,
        cd.editor_level,
        cd.editor_work_part,
        cqa.unit_price AS editor_unit_price
    FROM capture_processing cp
    LEFT JOIN capture_details cd
        ON cp.region_capture_id = cd.region_capture_id
    LEFT JOIN cqa_editors cqa
        ON cd.editor_email = cqa.email
),

-- 같은 task 그룹 내 캡처 수 계산
with_group_count AS (
    SELECT
        *,
        CASE
            WHEN region_team_id IS NOT NULL AND edit_started_at IS NOT NULL
            THEN COUNT(*) OVER (
                PARTITION BY region_team_id, edit_started_at
            )
            ELSE 1
        END AS same_task_capture_count
    FROM joined
),

final AS (
    SELECT
        region_capture_id,
        region_team_id,
        editor_email,
        editor_level,
        editor_unit_price,
        uploading_finished_at,
        edit_started_at,
        edit_finished_at,
        same_task_capture_count,
        FORMAT_TIMESTAMP('%Y-%m', uploading_finished_at) AS year_month,

        -- Processing 비용 (캡처별 개별)
        COALESCE(video_length, 0) * 0.00540839 AS processing_cost,

        -- Editing 인건비 (시급 → 분 변환 + 같은 task 캡처 수로 나누기)
        SAFE_DIVIDE(
            (
                COALESCE(edit_started_to_edit_finished_min, 0)
                + COALESCE(review_started_to_review_finished_min, 0)
            ) / 60 * COALESCE(editor_unit_price, 12.31),
            same_task_capture_count
        ) AS editing_labor_cost,

        -- 총 비용
        COALESCE(video_length, 0) * 0.00540839
        + SAFE_DIVIDE(
            (
                COALESCE(edit_started_to_edit_finished_min, 0)
                + COALESCE(review_started_to_review_finished_min, 0)
            ) / 60 * COALESCE(editor_unit_price, 12.31),
            same_task_capture_count
        ) AS total_capture_cost

    FROM with_group_count
)

SELECT * FROM final