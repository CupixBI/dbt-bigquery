/*
    stg_segment_events.sql
    
    목적: Segment 이벤트 로그 스테이징
    
    소스: cupixworks_raw.segment_events
    원본 컬럼: timestamp, region_code, tenant, messageId, event_json (JSON), _updated_at
    
    event_json에서 주요 필드를 추출하여 평탄화(flatten)
    나중에 다양한 분석에 재활용 가능하도록 최대한 많은 정보를 포함
    
    조인 키 통일:
      - region: context.region_code 사용 (uswe2/apse2/euce1 — 다른 테이블과 동일)
      - team_id, user_id, workspace_id: STRING 타입 (다른 stg 테이블과 동일)
      - region_team_id: "US-488" 형태 (다른 테이블과 동일)
*/

WITH source AS (
    SELECT * FROM {{ source('cupixworks', 'segment_events') }}
),

renamed AS (
    SELECT
        -- 메타
        messageId AS message_id,
        TIMESTAMP(timestamp) AS event_timestamp,
        TIMESTAMP(_updated_at) AS _updated_at,

        -- region (다른 테이블과 통일: uswe2, apse2, euce1)
        COALESCE(
            JSON_EXTRACT_SCALAR(event_json, '$.context.region_code'),
            region_code
        ) AS region,
        tenant,

        -- 이벤트 기본 정보
        JSON_EXTRACT_SCALAR(event_json, '$.type') AS event_type,
        JSON_EXTRACT_SCALAR(event_json, '$.event') AS event_name,
        JSON_EXTRACT_SCALAR(event_json, '$.channel') AS channel,

        -- 유저 정보 (STRING — 다른 stg 테이블과 동일)
        JSON_EXTRACT_SCALAR(event_json, '$.context.user_id') AS user_id,
        CASE
            WHEN JSON_EXTRACT_SCALAR(event_json, '$.context.user_id') IS NOT NULL THEN
                CONCAT(
                    CASE COALESCE(JSON_EXTRACT_SCALAR(event_json, '$.context.region_code'), region_code)
                        WHEN 'uswe2' THEN 'US'
                        WHEN 'apse2' THEN 'AU'
                        WHEN 'euce1' THEN 'EU'
                        WHEN 'apne1' THEN 'JP'
                        WHEN 'apse1' THEN 'SG'
                        WHEN 'cace1' THEN 'CA'
                        ELSE 'Unknown'
                    END,
                    '-',
                    JSON_EXTRACT_SCALAR(event_json, '$.context.user_id'),
                    '-',
                    tenant
                )
        END AS region_user_id,
        JSON_EXTRACT_SCALAR(event_json, '$.context.traits.user_email') AS user_email,
        JSON_EXTRACT_SCALAR(event_json, '$.context.traits.user_name') AS user_name,
        SAFE_CAST(JSON_EXTRACT_SCALAR(event_json, '$.context.traits.support_engineer') AS BOOLEAN) AS is_support_engineer,

        -- 팀 정보 (STRING — 다른 stg 테이블과 동일)
        JSON_EXTRACT_SCALAR(event_json, '$.context.team_id') AS team_id,
        -- region_team_id: "US-488" 형태 (다른 테이블과 동일한 조인 키)
        CONCAT(
            CASE COALESCE(JSON_EXTRACT_SCALAR(event_json, '$.context.region_code'), region_code)
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-',
            JSON_EXTRACT_SCALAR(event_json, '$.context.team_id'),
            '-',
            tenant
        ) AS region_team_id,
        -- 워크스페이스 정보 (STRING)
        JSON_EXTRACT_SCALAR(event_json, '$.context.workspace_id') AS workspace_id,
        CASE
            WHEN JSON_EXTRACT_SCALAR(event_json, '$.context.workspace_id') IS NOT NULL THEN
                CONCAT(
                    CASE COALESCE(JSON_EXTRACT_SCALAR(event_json, '$.context.region_code'), region_code)
                        WHEN 'uswe2' THEN 'US'
                        WHEN 'apse2' THEN 'AU'
                        WHEN 'euce1' THEN 'EU'
                        WHEN 'apne1' THEN 'JP'
                        WHEN 'apse1' THEN 'SG'
                        WHEN 'cace1' THEN 'CA'
                        ELSE 'Unknown'
                    END,
                    '-',
                    JSON_EXTRACT_SCALAR(event_json, '$.context.workspace_id'),
                    '-',
                    tenant
                )
        END AS region_workspace_id,
        JSON_EXTRACT_SCALAR(event_json, '$.context.workspace_name') AS workspace_name,

        -- 프로젝트(Facility) 정보
        JSON_EXTRACT_SCALAR(event_json, '$.context.facility_key') AS facility_key,
        JSON_EXTRACT_SCALAR(event_json, '$.context.facilityId') AS facility_id,
        CASE
            WHEN JSON_EXTRACT_SCALAR(event_json, '$.context.facilityId') IS NOT NULL THEN
                CONCAT(
                    CASE COALESCE(JSON_EXTRACT_SCALAR(event_json, '$.context.region_code'), region_code)
                        WHEN 'uswe2' THEN 'US'
                        WHEN 'apse2' THEN 'AU'
                        WHEN 'euce1' THEN 'EU'
                        WHEN 'apne1' THEN 'JP'
                        WHEN 'apse1' THEN 'SG'
                        WHEN 'cace1' THEN 'CA'
                        ELSE 'Unknown'
                    END,
                    '-',
                    JSON_EXTRACT_SCALAR(event_json, '$.context.facilityId'),
                    '-',
                    tenant
                )
        END AS region_facility_id,
        JSON_EXTRACT_SCALAR(event_json, '$.context.project_id') AS project_id,
        JSON_EXTRACT_SCALAR(event_json, '$.context.project_name') AS project_name,

        -- 페이지 정보
        JSON_EXTRACT_SCALAR(event_json, '$.context.page.path') AS page_path,
        JSON_EXTRACT_SCALAR(event_json, '$.context.page.url') AS page_url,
        JSON_EXTRACT_SCALAR(event_json, '$.context.page.referrer') AS page_referrer,

        -- 클라이언트 정보
        JSON_EXTRACT_SCALAR(event_json, '$.context.agent_version') AS agent_version,
        JSON_EXTRACT_SCALAR(event_json, '$.context.userAgent') AS user_agent,
        JSON_EXTRACT_SCALAR(event_json, '$.context.locale') AS locale,
        JSON_EXTRACT_SCALAR(event_json, '$.context.timezone') AS timezone,
        JSON_EXTRACT_SCALAR(event_json, '$.context.ip') AS ip_address,

        -- properties (track 이벤트용)
        JSON_EXTRACT_SCALAR(event_json, '$.properties.type') AS property_type,
        JSON_EXTRACT_SCALAR(event_json, '$.properties.model') AS property_model,
        JSON_EXTRACT_SCALAR(event_json, '$.properties.id') AS property_id,
        JSON_EXTRACT_SCALAR(event_json, '$.properties.action') AS property_action

    FROM source
)

SELECT * FROM renamed
WHERE
    -- track 이벤트만 포함 (시스템 이벤트 제외) 
    -- 아래 이벤트 리스트 구체화 필요
    event_type = 'track' AND event_name NOT IN (
        'SE_UPDATE_PANO', 'SE_CREATE_CAMERA', 'SE_CREATE_PANO',
        'WE_ENGINE_COMMAND', 'WE_SQA_EDITOR_CALL_SAVE', 'WE_SQA_EDITOR_CALL_SAVE_SUCCESS',
        'SE_UPDATE_CAPTURE', 'SE_UPDATE_REFERENCE', 'WE_MEASURE_PICK_OPTION_CHANGED_SITEVIEW',
        'SE_CREATE_REFERENCE', 'SE_UPDATE_POINTCLOUD', 'WE_SOLVE_CONSTRAINT',
        'SE_DELETED', 'SE_UPDATE_CLUSTER', 'SE_RUNNING_STATE_STOPPED', 'SE_RUNNING_STATE_RUNNING',
        'SE_CREATE_CLUSTER', 'Application Backgrounded', 'SE_PROCESSING_FINALIZED',
        'SE_RUNNING_STATE_STOPPING', 'SE_UPDATE_BIM', 'SE_SHARED', 'SE_EDITING_STATE_READY',
        'LE_UPLOAD_PANO_START', 'LE_UPLOAD_PANO_DONE', 'WE_DOLLHOUSE_SET_PANO_BUBBLE_SIZE',
        'WE_SV_API_GET_PANO', 'WE_SV_API_GET_PANO_ALL', 'WE_SV_API_FIND_NEAREST_PANOS',
        'WE_DOLLHOUSE_PANO_BUBBLE_VISIBILITY_TOGGLE', 'LE_UPLOAD_PANO_FAILURE',
        'WE_SV_API_CHANGE_PANO'
    )