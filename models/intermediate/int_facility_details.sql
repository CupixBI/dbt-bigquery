WITH facilities AS (
    SELECT * FROM {{ ref('stg_tesla__facilities') }}
),

workspaces AS (
    SELECT * FROM {{ ref('stg_tesla__workspaces') }}
),

teams AS (
    SELECT * FROM {{ ref('stg_tesla__teams') }}
),

final AS (
    SELECT
        -- 1. Facility (Grain)
        f.facility_id,
        f.facility_name,
        f.tenant,
        -- f.facility_address,
        f.created_at AS facility_created_at,
        -- Region 변환 로직
        f.region AS region,

        f.region_facility_id,
        f.state AS facility_state,
        f.cycle_state AS facility_cycle_state,
        f.facility_size,
        f.facility_size_unit,
        f.captured_size,

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
        t.domain AS team_domain,
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