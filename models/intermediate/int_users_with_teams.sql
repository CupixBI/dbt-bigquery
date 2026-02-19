WITH teams AS (
    SELECT * FROM {{ ref('stg_tesla__teams') }}
),

users AS (
    SELECT * FROM {{ ref('stg_tesla__users') }}
),

final AS (
    SELECT
        -- [Teams 정보]
        t.region AS team_region, -- 유저의 region과 같겠지만, 팀 기준 region임을 명시
        t.team_id,
        t.team_name,
        t.state AS team_state,
        t.cycle_state AS team_cycle_state,
        t.lock_state AS team_lock_state,
        t.lock_reason AS team_lock_reason,
        t.account_manager_email,
        t.primary_csm_email,
        t.secondary_csm_email,

        -- [Users 정보]
        u.user_id,
        u.user_email,
        u.full_name,
        u.state AS user_state,
        u.cycle_state AS user_cycle_state,
        u.first_sign_in_at,
        u.last_sign_in_at,
        u.current_sign_in_ip,
        u.last_sign_in_ip,
        u.created_at AS user_created_at

    FROM users u
    LEFT JOIN teams t
        ON u.region_team_id = t.region_team_id
)

SELECT * FROM final