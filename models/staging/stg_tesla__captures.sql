WITH source AS(
    SELECT * FROM {{ source('tesla', 'captures') }}
),

filtered AS(
    SELECT *
    FROM source
    WHERE team_id IS NOT NULL
),

-- 1단계: 구조 정리 (타입 변환 및 컬럼명 변경)
renamed AS(
    SELECT
        region,
        CAST(_id as STRING) as capture_id,
        CAST(camera_id as STRING) as camera_id,
        TIMESTAMP(captured_at) as captured_at,
        TIMESTAMP(created_at) as created_at,
        cycle_state,
        TIMESTAMP(cycle_state_updated_at) as cycle_state_updated_at,
        editing_difficulty_score,
        
        CAST(editing_entity_id as STRING) as editing_entity_id, 
        CAST(editing_id as STRING) as editing_id,
        editing_state,
        TIMESTAMP(editing_state_updated_at) as editing_state_updated_at,
        CAST(editor_id as STRING) as editor_id,
        error_code,
        expected_quality,
        CAST(facility_id as STRING) as facility_id,
        filesize,
        CAST(level_id as STRING) as level_id,
        CAST(workspace_id as STRING) as workspace_id,
        CAST(team_id as STRING) as team_id,
        name as capture_name,
        panos_count,
        processing_status,
        CAST(progress as FLOAT64) as progress,
        reconstruction_error_code,
        reconstruction_state,
        TIMESTAMP(reconstruction_state_updated_at) as reconstruction_state_updated_at,
        CAST(record_id as STRING) as record_id,
        refinement_error_code,
        refinement_floorplan_type,
        refinement_state,
        reprocess_count,
        running_state,
        CAST(source_capture_id as STRING) as source_capture_id,
        CAST(spacetime_id as STRING) as spacetime_id,
        state,
        TIMESTAMP(state_updated_at) as state_updated_at,
        timezone_offset,
        upload_platform,
        upload_state,
        TIMESTAMP(upload_state_updated_at) as upload_state_updated_at,
        
        -- [중요] user_id를 captured_by_user_id로 명확히 변경
        CAST(user_id as STRING) as captured_by_user_id,
        video_length
    FROM filtered
),

-- 2단계: Null 처리 및 비즈니스 로직
final AS (
    SELECT
        -- [기본 컬럼]
        region,
        capture_id,
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
            capture_id
        ) AS region_capture_id,
        
        captured_at,
        created_at,

        -- [1] FK 및 참조값 (NOT NULL이므로 그대로 사용)
        camera_id,
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
            camera_id
        ) AS region_camera_id,
        
        editing_entity_id,
        editing_id,
        editor_id,
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
            editor_id
        ) AS region_editor_id,
        
        -- [2] 상태(State) 값 (데이터 누락 가능성이 있다면 COALESCE 유지 추천)
        COALESCE(state, 'Unknown') AS state,
        COALESCE(cycle_state, 'Unknown') AS cycle_state,
        COALESCE(editing_state, 'Unknown') AS editing_state,
        COALESCE(reconstruction_state, 'Unknown') AS reconstruction_state,
        COALESCE(refinement_state, 'Unknown') AS refinement_state,
        COALESCE(running_state, 'Unknown') AS running_state,
        
        -- [3] 메타데이터 Null 처리
        COALESCE(upload_platform, 'Unknown') AS upload_platform,
        COALESCE(expected_quality, 'Unknown') AS expected_quality,
        
        -- [4] 수치값 Null 처리 (-1로 대체)
        COALESCE(filesize, -1) AS filesize,
        COALESCE(progress, -1) AS progress,

        -- [나머지 컬럼들]
        cycle_state_updated_at,
        editing_difficulty_score,
        editing_state_updated_at,
        error_code,
        
        -- Facility ID (NOT NULL)
        facility_id,
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
            facility_id
        ) AS region_facility_id,
        
        -- Level ID (NOT NULL)
        level_id,
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
            level_id
        ) AS region_level_id,
        
        -- Workspace ID (NOT NULL)
        workspace_id,
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
        
        -- Team ID (NOT NULL)
        team_id,
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
        
        -- Capture Name & Type
        COALESCE(capture_name, 'Unknown') AS capture_name,
        CASE 
            WHEN capture_name LIKE '%3D Map%' THEN '3D Map'
            WHEN capture_name LIKE '%Drive%' THEN 'Drive'
            WHEN capture_name LIKE '%Photo%' THEN 'Photo'
            WHEN capture_name LIKE '%Area%' THEN 'Area'
            WHEN capture_name LIKE '%Video%' THEN 'Video'
            ELSE 'Others'
        END AS capture_type,
        
        panos_count,
        processing_status,
        reconstruction_error_code,
        reconstruction_state_updated_at,
        
        record_id,
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
            record_id
        ) AS region_record_id,
        
        refinement_error_code,
        refinement_floorplan_type,
        reprocess_count,
        source_capture_id,
        spacetime_id,
        state_updated_at,
        timezone_offset,
        upload_state,
        upload_state_updated_at,
        
        -- User ID (NOT NULL) - 컬럼명 captured_by_user_id로 통일
        captured_by_user_id,
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
            captured_by_user_id
        ) AS region_captured_by_user_id,
        
        video_length

    FROM renamed
)

SELECT * FROM final