WITH captures AS (
    SELECT * FROM {{ ref('stg_tesla__captures') }}
),

users AS (
    SELECT 
        region_user_id,
        user_email ,
        tenant,
        region
    FROM {{ ref('stg_tesla__users') }}
),

cqa_editors AS (
    SELECT * FROM {{ ref('stg_finance__cqa_editors') }} 
),

-- [DISABLED] tenant 미포함 외부 소스 — region_capture_id 키 개편 후 재연결 필요
-- capture_issues_agg AS (
--     SELECT
--         region_capture_id,
--         STRING_AGG(DISTINCT issue_name, ', ') AS issue_names,
--         STRING_AGG(issue_code, ', ') AS issue_codes
--     FROM {{ ref('stg_monday__capture_issues') }}
--     GROUP BY 1
-- ),

-- re_edit_requested AS (
--     SELECT
--         region_capture_id,
--         COUNT(*) AS re_edit_count,
--         MAX(created_at) AS last_requested_at
--     FROM {{ ref('stg_slack__re_edit_requested') }}
--     GROUP BY 1
-- ),

teams AS (
    SELECT * FROM {{ ref('stg_tesla__teams') }}
),

facilities AS (
    SELECT * FROM {{ ref('stg_tesla__facilities') }}
),

cameras AS (
    SELECT * FROM {{ ref('stg_tesla__cameras')}}
),

workspaces AS (
    SELECT * FROM {{ ref('stg_tesla__workspaces') }}
),

filtered AS(
    SELECT *
    FROM captures
    WHERE video_length < 4000
    AND captured_by_user_email NOT LIKE '%cupix%'
    AND NOT (capture_type IN ('3D Map', 'Video') AND video_length = 0)
),

final AS(
    SELECT
        captures.region,
        captures.created_at,
        captures.tenant,
        TIMESTAMP_ADD(captures.created_at, INTERVAL 9 HOUR) AS created_at_kst,
        captures.region_capture_id,
        captures.capture_trace_id,
        captures.region_capture_trace_id,
        captures.cycle_state,
        captures.state,
        captures.editing_state,
        captures.editing_id,
        captures.editor_id,
        captures.error_code,
        captures.reconstruction_state,
        captures.refinement_state,
        captures.running_state,
        captures.progress,
        captures.capture_type,
        captures.processing_status,
        captures.reconstruction_error_code,
        captures.refinement_error_code,
        captures.refinement_floorplan_type,
        captures.reprocess_count,
        captures.upload_state,
        captures.video_length,
        captures.captured_by_user_email,
        captures.region_level_id,
        captures.region_record_id,

        captures.region_editor_id,
        users.user_email AS editor_email,
        cqa.editor_name,
        cqa.level AS editor_level,
        cqa.work_part AS editor_work_part,
        cqa.unit_price AS editor_unit_price,

        -- [DISABLED] tenant 미포함 외부 소스 — region_capture_id 키 개편 후 재연결 필요
        -- capture_issues_agg.issue_names,
        -- capture_issues_agg.issue_codes,
        -- COALESCE(re_edit_requested.re_edit_count, 0) as re_edit_count,
        -- re_edit_requested.last_requested_at,
        -- CASE
        --     WHEN re_edit_requested.re_edit_count > 0 THEN TRUE
        --     ELSE FALSE
        -- END AS is_re_edited,
        
        teams.team_name,
        teams.region_team_id,

        facilities.facility_name,
        facilities.region_facility_id,
        facilities.bims_count,

        workspaces.workspace_name,
        workspaces.region_workspace_id,

        cameras.camera_id,
        cameras.model_name AS camera_model_name

    FROM filtered AS captures
    
    -- [DISABLED] tenant 미포함 외부 소스 — region_capture_id 키 개편 후 재연결 필요
    -- LEFT JOIN re_edit_requested
    --     ON captures.region_capture_id = re_edit_requested.region_capture_id
    -- LEFT JOIN capture_issues_agg
    --     ON captures.region_capture_id = capture_issues_agg.region_capture_id
    
    LEFT JOIN teams
        ON captures.region_team_id = teams.region_team_id

    LEFT JOIN facilities
        ON captures.region_facility_id = facilities.region_facility_id

    LEFT JOIN users
        ON captures.region_editor_id = users.region_user_id

    LEFT JOIN cqa_editors cqa
        ON users.user_email = cqa.email

    LEFT JOIN cameras
        ON captures.region_camera_id = cameras.region_camera_id

    LEFT JOIN workspaces
        ON captures.region_workspace_id = workspaces.region_workspace_id
)

SELECT * FROM final