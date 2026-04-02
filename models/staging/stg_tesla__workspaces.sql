WITH source AS (
    SELECT * FROM {{ source('tesla', 'workspaces') }}
),

-- 1단계: 이름 변경 및 타입 변환
renamed AS (
    SELECT 
        region,
        tenant,
        CAST(_id as STRING) as workspace_id,
        name as workspace_name,
        CAST(user_id as STRING) as created_by_user_id,
        
        -- team_id는 NOT NULL이므로 변환만 수행
        CAST(team_id as STRING) as team_id,
        
        state,
        TIMESTAMP(created_at) as created_at,
        updated_at, 
        cycle_state,
        TIMESTAMP(cycle_state_updated_at) as cycle_state_updated_at,
        CAST(quote_id as STRING) as quote_id,
        lock_state,
        lock_reason,
        TIMESTAMP(lock_state_updated_at) as lock_state_updated_at
    FROM source
),

-- 2단계: Null 처리 및 비즈니스 로직
final AS (
    SELECT
        region,
        workspace_id,
        tenant,
        
        -- [Workspace ID] Region Prefix
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
        
        COALESCE(workspace_name, 'Unknown') as workspace_name,
        COALESCE(created_by_user_id, 'Unknown') as created_by_user_id,
        
        -- [수정] team_id는 Null 처리가 필요 없음
        team_id,
        
        -- [Team ID] Region Prefix
        -- team_id가 NOT NULL이므로 COALESCE 없이 바로 연결
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
        
        COALESCE(state, 'Unknown') as state,
        created_at,
        updated_at,
        COALESCE(cycle_state, 'Unknown') as cycle_state,
        cycle_state_updated_at,
        quote_id,
        COALESCE(lock_state, 'Unknown') as lock_state,

        -- Lock Reason 로직
        COALESCE(lock_reason, 
            CASE 
                WHEN lock_state = 'active' THEN 'active'
                WHEN lock_state = 'locked' THEN 'locked'
                ELSE 'Unknown'
            END
        ) AS lock_reason,

        lock_state_updated_at
    FROM renamed
)

SELECT * FROM final