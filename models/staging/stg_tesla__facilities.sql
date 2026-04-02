WITH source AS (
    SELECT * FROM {{ source('tesla', 'facilities') }}
),

renamed AS (
    SELECT 
        region,
        CAST(_id as STRING) as facility_id,
        name as facility_name,
        TIMESTAMP(created_at) as created_at, 
        state,
        cycle_state,
        tenant,
        TIMESTAMP(cycle_state_updated_at) as cycle_state_updated_at,
        facility_size,
        facility_size_unit,
        captured_size,
        CAST(quote_id as STRING) as quote_id,
        timezone_offset,
        CAST(workspace_id as STRING) as workspace_id,
        CAST(user_id as STRING) as created_by_user_id,
        last_captured_at,
        CAST(team_id as STRING) as team_id,
        
        -- [추가] sys 컬럼에서 address 키의 값만 추출 (JSON 파싱)
        -- JSON_EXTRACT_SCALAR(sys, '$.address') as facility_address
        -- sys.address as facility_address

    FROM source
),

final AS (
    SELECT
        region,
        facility_id,
        tenant,
        -- Region Prefix
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
            facility_id
        ) AS region_facility_id,
        
        COALESCE(facility_name, 'Unknown') AS facility_name,
        created_at,
        COALESCE(state, 'Unknown') AS state,
        COALESCE(cycle_state, 'Unknown') AS cycle_state,
        cycle_state_updated_at,
        COALESCE(facility_size, -1) AS facility_size,
        COALESCE(facility_size_unit, 'Unknown') AS facility_size_unit,
        COALESCE(captured_size, 0) AS captured_size,
        quote_id,
        
        -- [추가] 주소 정보 (Null이면 Unknown 처리)
        -- COALESCE(facility_address, 'Unknown') AS facility_address,
        
        timezone_offset,
        workspace_id,
        
        -- Region Workspace ID
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
            workspace_id
        ) AS region_workspace_id,

        team_id,

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
    team_id
) AS region_team_id,
        
        
        COALESCE(created_by_user_id, 'Unknown') AS created_by_user_id,
        last_captured_at
    FROM renamed
)

SELECT * FROM final