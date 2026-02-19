WITH facilities AS (
    SELECT * FROM {{ ref('stg_tesla__facilities') }}
),

workspaces AS (
    SELECT * FROM {{ ref('stg_tesla__workspaces') }}
),

teams AS (
    -- 이미 이메일 컬럼(account_manager_email 등)이 포함되어 있습니다.
    SELECT * FROM {{ ref('stg_tesla__teams') }}
),

final AS (
    SELECT
        -- 1. Facility (Grain)
        f.facility_id,
        f.facility_name,
        f.created_at AS facility_created_at,
        
        -- Region 변환 로직
        CASE f.region
            WHEN 'uswe2' THEN 'US'
            WHEN 'apse2' THEN 'AU'
            WHEN 'euce1' THEN 'EU'
            WHEN 'apne1' THEN 'JP'
            WHEN 'apse1' THEN 'SG'
            WHEN 'cace1' THEN 'CA'
            ELSE 'Unknown'
        END AS region,
        
        f.state AS facility_state,
        f.cycle_state AS facility_cycle_state,
        f.facility_size,
        f.facility_size_unit,

        -- 2. Workspace
        w.workspace_id,
        w.workspace_name,
        w.state AS workspace_state,
        w.cycle_state AS workspace_cycle_state,
        w.lock_state AS workspace_lock_state,
        w.lock_reason AS workspace_lock_reason,

        -- 3. Team
        t.team_id,
        t.team_name,
        t.state AS team_state,
        t.cycle_state AS team_cycle_state,
        t.lock_state AS team_lock_state,
        t.lock_reason AS team_lock_reason,
        t.account_manager_email,
        t.primary_csm_email,
        t.secondary_csm_email

    FROM facilities f
    
    -- Facility -> Workspace 조인
    LEFT JOIN workspaces w 
        ON f.region_workspace_id = w.region_workspace_id
        
    -- Workspace -> Team 조인
    LEFT JOIN teams t 
        ON w.region_team_id = t.region_team_id
)

SELECT * FROM final