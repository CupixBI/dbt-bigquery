WITH source AS (
    SELECT * FROM {{ source('tesla', 'levels') }}
),

renamed AS(
    SELECT
        region,
        CAST(_id as STRING) as level_id,
        name as level_name,
        state,
        CAST(user_id as STRING) as created_by_user_id,
        
        -- facility_id는 NOT NULL이라고 가정
        CAST(facility_id as STRING) as facility_id,
        
        -- [수정] 원본이 이미 TIMESTAMP이므로 함수 제거하고 그대로 사용
        created_at,
        cycle_state,
        cycle_state_updated_at,
        tenant
    FROM source
),

final AS(
    SELECT
        region,
        level_id,
        tenant,

        -- [Level ID] Region Prefix
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
            level_id,
            '-',
            tenant
        ) AS region_level_id,
        
        COALESCE(level_name, 'Unknown') as level_name,
        COALESCE(state, 'Unknown') as state,
        COALESCE(created_by_user_id, 'Unknown') as created_by_user_id,
        
        facility_id,
        
        -- [Facility ID] Region Prefix (조인 키)
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
            facility_id,
            '-',
            tenant
        ) AS region_facility_id,
        
        created_at,
        COALESCE(cycle_state, 'Unknown') AS cycle_state,
        cycle_state_updated_at
    FROM renamed
)

SELECT * FROM final