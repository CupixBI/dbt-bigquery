WITH source AS (
    SELECT * FROM {{ source('tesla', 'users') }}
),

-- 1단계: 이름 변경 및 타입 변환
renamed AS (
    SELECT
        region,
        CAST(_id as STRING) as user_id,
        email as user_email,
        firstname,
        lastname,
        locale,
        state,
        cycle_state,
        TIMESTAMP(cycle_state_updated_at) as cycle_state_updated_at,
        
        -- team_id는 NOT NULL이라 가정하므로 단순히 String 변환만 수행
        CAST(team_id as STRING) as team_id,
        
        editor_level,
        TIMESTAMP(first_sign_in_at) as first_sign_in_at,
        TIMESTAMP(last_sign_in_at) as last_sign_in_at,
        current_sign_in_ip,
        last_sign_in_ip,
        slack_id,
        TIMESTAMP(created_at) as created_at
    FROM source
),

final AS(
    SELECT
        region,
        user_id,
        
        -- [User ID] Region Prefix
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
            user_id
        ) AS region_user_id,

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

        COALESCE(user_email, 'Unknown') as user_email,
        
        -- 이름 정보 (Null 처리 유지)
        COALESCE(firstname, 'Unknown') as firstname,
        COALESCE(lastname, 'Unknown') as lastname,
        TRIM(CONCAT(COALESCE(firstname, ''), ' ', COALESCE(lastname, ''))) as full_name,

        locale,
        COALESCE(state, 'Unknown') as state,
        COALESCE(cycle_state, 'Unknown') as cycle_state,
        cycle_state_updated_at,
        
        -- [수정됨] Null 처리를 제거하고 원본 그대로 사용
        team_id,
        
        editor_level,
        first_sign_in_at,
        last_sign_in_at,
        current_sign_in_ip,
        last_sign_in_ip,
        COALESCE(slack_id, 'Unknown') as slack_id,
        created_at
    FROM renamed
)

SELECT * FROM final