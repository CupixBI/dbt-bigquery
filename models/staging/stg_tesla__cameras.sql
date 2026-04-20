WITH source AS (
    SELECT * FROM {{ source('tesla', 'cameras') }}
),

renamed AS(
    SELECT
        region,
        CAST(_id as STRING) as camera_id,
        make as manufacturer,
        model as model_name,
        software,
        serial_number,
        state,
        created_at,
        tenant,
    FROM source
),

final AS(
    SELECT
        region,
        camera_id,
        tenant,
        
        -- [Camera ID] Region Prefix
        CONCAT(
            CASE region
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-',
            camera_id,
            '-',
            tenant
        ) AS region_camera_id,
        
        -- [수정] 큰따옴표(") -> 작은따옴표(') 통일 & 오타 수정
        COALESCE(manufacturer, 'Unknown') as manufacturer,
        COALESCE(model_name, 'Unknown') as model_name,
        COALESCE(software, 'Unknown') as software,
        COALESCE(serial_number, 'Unknown') as serial_number,
        
        -- [수정] state에도 안전하게 Null 처리 추가
        COALESCE(state, 'Unknown') as state,
        
        created_at
    FROM renamed
)

SELECT * FROM final