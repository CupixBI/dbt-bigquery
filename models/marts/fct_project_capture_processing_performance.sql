WITH capture_processing AS (
    SELECT * FROM {{ ref('int_capture_processing') }}
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
        cd.error_code,
        cd.reprocess_count
    FROM capture_processing cp
    LEFT JOIN capture_details cd
        ON cp.region_capture_id = cd.region_capture_id
),

final AS (
    SELECT
        team_name,
        project_name,
        project_id,

        -- Captures
        COUNT(DISTINCT region_capture_id) AS captures,

        -- Delivery SLA
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE
                WHEN has_cpc AND cpc_delivery_lead_time_min / 60 <= 4 THEN region_capture_id
                WHEN NOT has_cpc AND sv_delivery_lead_time_min / 60 <= 4 THEN region_capture_id
            END),
            COUNT(DISTINCT region_capture_id)
        ) AS delivery_sla_rate,

        -- Rework Rate
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE
                WHEN post_process_count >= 3
                OR re_edit_count > 0
                OR reconstruction_process_count > 1
                THEN region_capture_id
            END),
            COUNT(DISTINCT region_capture_id)
        ) AS rework_rate,

        -- Recalculation Rate
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE
                WHEN reprocess_count > 0
                THEN region_capture_id
            END),
            COUNT(DISTINCT region_capture_id)
        ) AS recalculation_rate,

        -- Avg Edit Time / 1min video
        SAFE_DIVIDE(
            AVG(first_edit_duration_min),
            AVG(video_length / 60)
        ) AS avg_edit_time_per_1min_video,

        -- Error Rate
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE
                WHEN error_code IS NOT NULL
                THEN region_capture_id
            END),
            COUNT(DISTINCT region_capture_id)
        ) AS error_rate,

        -- Status
        CASE
            WHEN SAFE_DIVIDE(
                COUNT(DISTINCT CASE
                    WHEN has_cpc AND cpc_delivery_lead_time_min / 60 <= 4 THEN region_capture_id
                    WHEN NOT has_cpc AND sv_delivery_lead_time_min / 60 <= 4 THEN region_capture_id
                END),
                COUNT(DISTINCT region_capture_id)
            ) >= 0.95 THEN 'TARGET'
            WHEN SAFE_DIVIDE(
                COUNT(DISTINCT CASE
                    WHEN has_cpc AND cpc_delivery_lead_time_min / 60 <= 4 THEN region_capture_id
                    WHEN NOT has_cpc AND sv_delivery_lead_time_min / 60 <= 4 THEN region_capture_id
                END),
                COUNT(DISTINCT region_capture_id)
            ) >= 0.90 THEN 'WARNING'
            ELSE 'ACTION NEEDED'
        END AS status

    FROM joined
    GROUP BY
        project_id,
        project_name,
        team_name
)

SELECT * FROM final