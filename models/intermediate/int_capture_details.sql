WITH captures AS (
    SELECT * FROM {{ ref('stg_tesla__captures') }}
),

-- [추가됨] Users 테이블 (에디터 이메일 확인용)
users AS (
    SELECT 
        region_user_id,
        user_email 
    FROM {{ ref('stg_tesla__users') }}
),

-- [추가됨] CQA Editors 테이블 (에디터 상세 정보)
cqa_editors AS (
    SELECT * FROM {{ ref('stg_finance__cqa_editors') }} 
),

capture_issues_agg AS (
    SELECT 
        region_capture_id,
        STRING_AGG(DISTINCT issue_name, ', ') AS issue_names, 
        STRING_AGG(issue_code, ', ') AS issue_codes
    FROM {{ ref('stg_monday__capture_issues') }}
    GROUP BY 1
),

re_edit_requested AS (
    SELECT
        region_capture_id, 
        COUNT(*) AS re_edit_count, 
        MAX(created_at) AS last_requested_at 
    FROM {{ ref('stg_slack__re_edit_requested') }}
    GROUP BY 1
),

teams AS (
    SELECT * FROM {{ ref('stg_tesla__teams') }}
),

facilities AS (
    SELECT * FROM {{ ref('stg_tesla__facilities') }}
),

-- captures 테이블 기본 필터링
filtered AS(
    SELECT *
    FROM captures
    WHERE video_length < 4000
    AND captured_by_user_email NOT LIKE '%cupix%'
    AND NOT (capture_type IN ('3D Map', 'Video') AND video_length = 0)
),

final AS(
    SELECT
        -- Captures 기본 정보
        captures.region,
        captures.created_at,
        TIMESTAMP_ADD(captures.created_at, INTERVAL 9 HOUR) AS created_at_kst,
        captures.region_capture_id,
        captures.cycle_state,
        captures.editing_state,
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
        -- Creator 정보
        captures.captured_by_user_email,

        -- [추가됨] Editor 정보
        -- 1. Users 테이블에서 가져온 이메일
        users.user_email AS editor_email,
        -- 2. Finance CQA Editors 테이블에서 가져온 상세 정보
        cqa.editor_name,
        cqa.level AS editor_level,
        cqa.work_part AS editor_work_part,
        cqa.unit_price AS editor_unit_price,

        -- capture_issues_agg
        capture_issues_agg.issue_names,
        capture_issues_agg.issue_codes,

        -- re_edit_requested
        COALESCE(re_edit_requested.re_edit_count, 0) as re_edit_count,
        re_edit_requested.last_requested_at,
        CASE 
            WHEN re_edit_requested.re_edit_count > 0 THEN TRUE 
            ELSE FALSE 
        END AS is_re_edited,
        
        -- teams
        teams.team_name,
        teams.region_team_id,

        -- facilities
        facilities.facility_name,
        -- facilities.facility_address,
        facilities.region_facility_id

    FROM filtered AS captures
    
    -- 1. Re-edit Requested
    LEFT JOIN re_edit_requested
        ON captures.region_capture_id = re_edit_requested.region_capture_id

    -- 2. Capture Issues Aggregated
    LEFT JOIN capture_issues_agg
        ON captures.region_capture_id = capture_issues_agg.region_capture_id
    
    -- 3. Teams
    LEFT JOIN teams
        ON captures.region_team_id = teams.region_team_id
        
    -- 4. Facilities
    LEFT JOIN facilities
        ON captures.region_facility_id = facilities.region_facility_id

    -- [추가됨] 5. Users (Get Editor Email)
    LEFT JOIN users
        ON captures.region_editor_id = users.region_user_id

    -- [추가됨] 6. CQA Editors (Get Editor Details)
    -- 위에서 가져온 users.user_email과 cqa_editors.email을 조인합니다.
    LEFT JOIN cqa_editors cqa
        ON users.user_email = cqa.email
)

SELECT * FROM final