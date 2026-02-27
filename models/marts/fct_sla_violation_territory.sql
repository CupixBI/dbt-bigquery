WITH cp AS (
    SELECT 
        region_capture_id, 
        total_lead_time_min, 
        cpc_delivery_lead_time_min, 
        sv_delivery_lead_time_min,
        reconstruction_process_count,
        video_length    
    FROM {{ ref('int_capture_processing') }}
),

capture_details AS (
    SELECT region_capture_id, capture_type FROM {{ ref('int_capture_details') }}  
),

joined AS (
    SELECT
        LEFT(cp.region_capture_id, 2) AS region,
        cp.region_capture_id,
        cp.cpc_delivery_lead_time_min,
        cp.sv_delivery_lead_time_min,
        cp.reconstruction_process_count,
        cd.capture_type
    FROM cp
    LEFT JOIN capture_details cd ON cp.region_capture_id = cd.region_capture_id
),

final AS (
    SELECT
        region,

        -- 3D Map
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE
                WHEN capture_type = '3D Map'
                AND cpc_delivery_lead_time_min / 60 > 8
                THEN region_capture_id
            END),
            COUNT(DISTINCT region_capture_id)
        ) AS sla_breach_rate_3d_map,

        -- Drive
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE
                WHEN capture_type = 'Drive'
                AND sv_delivery_lead_time_min / 60 > 8
                THEN region_capture_id
            END),
            COUNT(DISTINCT region_capture_id)
        ) AS sla_breach_rate_drive,

        -- Photo
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE
                WHEN capture_type = 'Photo'
                AND sv_delivery_lead_time_min / 60 > 8
                THEN region_capture_id
            END),
            COUNT(DISTINCT region_capture_id)
        ) AS sla_breach_rate_photo,

        -- Area
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE
                WHEN capture_type = 'Area'
                AND sv_delivery_lead_time_min / 60 > 8
                THEN region_capture_id
            END),
            COUNT(DISTINCT region_capture_id)
        ) AS sla_breach_rate_area,

        -- Video
        SAFE_DIVIDE(
            COUNT(DISTINCT CASE
                WHEN capture_type = 'Video'
                AND sv_delivery_lead_time_min / 60 > 8
                THEN region_capture_id
            END),
            COUNT(DISTINCT region_capture_id)
        ) AS sla_breach_rate_video

    FROM joined
    GROUP BY 1
)

SELECT * FROM final