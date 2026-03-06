WITH source AS (
    SELECT
        cp.region_capture_id,
        cp.region_capture_trace_id,
        cp.capture_type,
        cp.video_length,
        cp.created_at_kst,
        cp.uploading_finished_at,
        cp.postprocessor_agent_finished_at,
        cp.edit_started_at,
        cp.cpc_delivery_lead_time_min,
        cp.sv_delivery_lead_time_min,
        cp.is_sla_exceeded,
        cp.has_cpc,
        cp.pre_process_count,
        cp.master_process_count,
        cp.post_process_count,
        cp.re_edit_count,
        cp.reconstruction_process_count,
        cp.uploading_to_processing_finished_min,
        cp.processing_finished_to_preview_finished_min,
        cp.preview_finished_to_edit_started_min,
        cp.edit_started_to_edit_finished_min,
        cp.review_started_to_review_finished_min,
        cp.total_lead_time_min,
        cp.error_code,
        cp.video_length_range,
        cp.reconstruction_error_code,
        DATE_TRUNC(CAST(cp.uploading_finished_at AS DATE), WEEK(MONDAY)) 
    = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY)) 
    AS is_last_week,

        -- 추가 메타데이터
        cd.team_name,
        cd.region_team_id,
        cd.facility_name AS project_name,
        cd.region_facility_id AS project_id,
        cd.captured_by_user_email AS creator,
        cd.editor_email AS editor,
        cd.region AS region

    FROM {{ ref('int_capture_processing') }} cp
    LEFT JOIN {{ ref('int_capture_details') }} cd
        ON cp.region_capture_id = cd.region_capture_id
)

SELECT * FROM source