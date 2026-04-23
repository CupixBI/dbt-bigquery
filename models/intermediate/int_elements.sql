WITH elements AS (
    SELECT * FROM {{ ref('stg_tesla__elements') }}
),

facilities AS (
    SELECT
        region_facility_id,
        facility_name
    FROM {{ ref('stg_tesla__facilities') }}
),

teams AS (
    SELECT
        region_team_id,
        team_name
    FROM {{ ref('stg_tesla__teams') }}
),

workspaces AS (
    SELECT
        region_workspace_id,
        workspace_name
    FROM {{ ref('stg_tesla__workspaces') }}
),

levels AS (
    SELECT
        region_level_id,
        level_name
    FROM {{ ref('stg_tesla__levels') }}
),

users AS (
    SELECT
        region_user_id,
        full_name AS user_name
    FROM {{ ref('stg_tesla__users') }}
),

bims AS (
    SELECT
        region_bim_id,
        name AS bim_name
    FROM {{ ref('stg_tesla__bims') }}
),

final AS (
    SELECT
        e.*,
        f.facility_name,
        t.team_name,
        w.workspace_name,
        l.level_name,
        u.user_name,
        b.bim_name
    FROM elements e
    LEFT JOIN facilities f
        ON e.region_facility_id = f.region_facility_id
    LEFT JOIN teams t
        ON e.region_team_id = t.region_team_id
    LEFT JOIN workspaces w
        ON e.region_workspace_id = w.region_workspace_id
    LEFT JOIN levels l
        ON e.region_level_id = l.region_level_id
    LEFT JOIN users u
        ON e.region_user_id = u.region_user_id
    LEFT JOIN bims b
        ON e.region_bim_id = b.region_bim_id
)

SELECT * FROM final
