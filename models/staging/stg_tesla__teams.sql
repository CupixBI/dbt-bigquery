WITH teams_source AS (
    SELECT * FROM {{ source('tesla', 'teams') }}
),

-- [추가됨] 이메일을 가져오기 위해 Users 소스도 필요합니다.
users_source AS (
    SELECT 
        _id, 
        region, 
        email 
    FROM {{ source('tesla', 'users') }}
),

-- 1단계: 이름 변경 및 타입 변환
renamed AS (
    SELECT
        region,
        CAST(_id as STRING) AS team_id,
        TIMESTAMP(created_at) AS created_at,
        updated_at,
        name as team_name,
        domain,
        state,
        locale,
        lock_state,
        TIMESTAMP(lock_state_updated_at) AS lock_state_updated_at,
        lock_reason,
        cycle_state,
        TIMESTAMP(cycle_state_updated_at) AS cycle_state_updated_at,
        CAST(user_id as STRING) as created_by_user_id,
        
        -- ID들은 조인을 위해 STRING으로 변환
        CAST(account_manager_id as STRING) as account_manager_id,
        CAST(primary_csm_id as STRING) as primary_csm_id,
        CAST(secondary_csm_id as STRING) as secondary_csm_id,
        
        CAST(quote_id as STRING) as quote_id,
        sf_resource_id,
        timezone_offset,
        TIMESTAMP(infosphere_builtin_enabled_at) AS infosphere_builtin_enabled_at
    FROM teams_source
),

-- [추가됨] 2단계: 이메일 정보 붙이기 (Join)
-- Staging 단계이므로 비즈니스 로직 적용 전에 Raw 데이터끼리 먼저 붙입니다.
joined_users AS (
    SELECT
        t.*,
        u_am.email AS account_manager_email,
        u_csm1.email AS primary_csm_email,
        u_csm2.email AS secondary_csm_email
    FROM renamed t
    
    -- 1. Account Manager Join (ID + Region)
    LEFT JOIN users_source u_am
        ON t.account_manager_id = CAST(u_am._id AS STRING)
        AND t.region = u_am.region

    -- 2. Primary CSM Join
    LEFT JOIN users_source u_csm1
        ON t.primary_csm_id = CAST(u_csm1._id AS STRING)
        AND t.region = u_csm1.region

    -- 3. Secondary CSM Join
    LEFT JOIN users_source u_csm2
        ON t.secondary_csm_id = CAST(u_csm2._id AS STRING)
        AND t.region = u_csm2.region
),

-- 3단계: Null 처리 및 파생 컬럼 생성
final AS (
    SELECT
        region,
        team_id,
        -- Region Prefix 로직
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
        created_at,
        updated_at,
        
        -- 1) team_name 처리
        COALESCE(team_name, 'Unknown') AS team_name,
        
        -- [수정됨] 쉼표 누락 수정
        COALESCE(domain, 'Unknown') as domain,
        COALESCE(state, 'Unknown') as state,
        locale,
        COALESCE(lock_state, 'Unknown') as lock_state,
        lock_state_updated_at,

        -- 2) lock_reason 로직
        COALESCE(lock_reason, 
            CASE 
                WHEN lock_state = 'active' THEN 'active'
                WHEN lock_state = 'locked' THEN 'locked'
                ELSE 'Unknown'
            END
        ) AS lock_reason,

        -- [수정됨] 쉼표 누락 수정
        COALESCE(cycle_state, 'Unknown') as cycle_state,
        cycle_state_updated_at,

        -- 3) ID값 Null 처리
        COALESCE(created_by_user_id, 'Unknown') as created_by_user_id,
        COALESCE(account_manager_id, 'Unknown') as account_manager_id,
        COALESCE(primary_csm_id, 'Unknown') as primary_csm_id,
        COALESCE(secondary_csm_id, 'Unknown') as secondary_csm_id,
        
        -- [추가됨] 이메일 컬럼 (Null이면 Unknown 처리)
        COALESCE(account_manager_email, 'Unknown') as account_manager_email,
        COALESCE(primary_csm_email, 'Unknown') as primary_csm_email,
        COALESCE(secondary_csm_email, 'Unknown') as secondary_csm_email,

        quote_id,

        COALESCE(sf_resource_id, 'Unknown') as sf_resource_id,
        timezone_offset,
        infosphere_builtin_enabled_at,
        infosphere_builtin_enabled_at IS NOT NULL AS infosphere_builtin_enablement

    FROM joined_users
)

SELECT * FROM final