WITH capture_processing AS (
    SELECT * EXCEPT(error_code)
    FROM {{ ref('int_capture_processing') }}
),

capture_details AS (
    SELECT * FROM {{ ref('int_capture_details') }}  
),

joined AS (
    SELECT
        cp.*,
        cd.team_name,
        cd.region_team_id,
        cd.facility_name AS project_name,
        cd.region_facility_id AS project_id,
        cd.error_code AS detail_error_code,
        cd.reprocess_count
    FROM capture_processing cp
    LEFT JOIN capture_details cd
        ON cp.region_capture_id = cd.region_capture_id
),

-- ✅ SLA 기준을 프로젝트 단위로 명시적으로 계산
project_level AS (
    SELECT
        project_id,
        COUNT(DISTINCT region_capture_id) AS total_captures,

        COUNT(DISTINCT CASE
            WHEN (has_cpc AND cpc_delivery_lead_time_min <= 480)
              OR (NOT has_cpc AND sv_delivery_lead_time_min <= 360)
            THEN region_capture_id
        END) AS sla_compliant_captures,

        COUNT(DISTINCT CASE
            WHEN post_process_count >= 3
              OR re_edit_count > 0
              OR reconstruction_process_count > 1
            THEN region_capture_id
        END) AS rework_captures,

        COUNT(DISTINCT CASE
            WHEN reprocess_count > 0 THEN region_capture_id
        END) AS recalculated_captures,

        COUNT(DISTINCT CASE
            WHEN detail_error_code IS NOT NULL THEN region_capture_id
        END) AS error_captures,

        AVG(edit_started_to_edit_finished_min) AS avg_edit_duration_min,
        AVG(video_length / 60) AS avg_video_length_min
    FROM joined
    GROUP BY project_id
)

SELECT
    j.team_name,
    j.project_name,
    j.project_id,

    p.total_captures AS captures,

    SAFE_DIVIDE(p.sla_compliant_captures, p.total_captures) AS delivery_sla_rate,
    SAFE_DIVIDE(p.rework_captures, p.total_captures) AS rework_rate,
    SAFE_DIVIDE(p.recalculated_captures, p.total_captures) AS recalculation_rate,
    SAFE_DIVIDE(p.avg_edit_duration_min, p.avg_video_length_min) AS avg_edit_time_per_1min_video,
    SAFE_DIVIDE(p.error_captures, p.total_captures) AS error_rate,

    CASE
        WHEN SAFE_DIVIDE(p.sla_compliant_captures, p.total_captures) >= 0.95 THEN 'TARGET'
        WHEN SAFE_DIVIDE(p.sla_compliant_captures, p.total_captures) >= 0.90 THEN 'WARNING'
        ELSE 'ACTION NEEDED'
    END AS status

FROM project_level p
LEFT JOIN joined j USING (project_id)
GROUP BY
    j.project_id,
    j.project_name,
    j.team_name,
    p.total_captures,
    p.sla_compliant_captures,
    p.rework_captures,
    p.recalculated_captures,
    p.avg_edit_duration_min,
    p.avg_video_length_min,
    p.error_captures