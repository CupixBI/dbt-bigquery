WITH facilities AS (
    SELECT * FROM {{ ref('stg_tesla__facilities') }}
),

teams AS (
    SELECT 
        region_team_id,
        infosphere_builtin_enabled_at,
        infosphere_builtin_enablement, 
        lock_state
    FROM {{ ref('stg_tesla__teams') }}
),

final AS (
    SELECT
        f.*,
        t.infosphere_builtin_enablement,
        t.lock_state,
        t.infosphere_builtin_enabled_at
    FROM facilities f
    LEFT JOIN teams t ON f.region_team_id = t.region_team_id
)

SELECT * from final