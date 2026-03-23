with
    source as (
        select
            cp.region_capture_id,
            cp.region_capture_trace_id,
            cp.capture_type,
            cp.video_length,
            cp.created_at_kst,
            cp.uploading_finished_at,
            cp.postprocessor_agent_finished_at,
            cp.edit_started_at,
            cp.edit_process_count,
            cp.is_holded,
            cp.is_escalated,
            cp.cpc_delivery_lead_time_min,
            cp.sv_delivery_lead_time_min,
            cp.is_sla_exceeded,
            cp.has_cpc,
            cp.edit_finished_to_review_started_min,
            cp.pre_process_count,
            cp.master_process_count,
            cp.post_process_count,
            cp.first_cpc_generation_duration_min,
            cp.re_edit_count,
            cp.reconstruction_process_count,
            cp.uploading_to_processing_finished_min,
            cp.processing_finished_to_preview_finished_min,
            cp.preview_finished_to_edit_started_min,
            cp.edit_started_to_edit_finished_min,
            cp.edit_finished_at,
            cp.review_started_to_review_finished_min,
            cp.total_lead_time_min,
            cp.error_code,
            cp.video_length_range,
            cp.reconstruction_error_code,
            date_trunc(
                cast(cp.uploading_finished_at as date),
                week(monday)
            ) = date_trunc(
                date_sub(current_date(), interval 1 week), week(monday)
            ) as is_last_week,

            -- 추가 메타데이터
            cd.team_name,
            cd.region_team_id,
            cd.facility_name as project_name,
            cd.region_facility_id as project_id,
            cd.captured_by_user_email as creator,
            cd.editor_email as editor_email,
            cd.editor_level,
            cd.editor_work_part,
            cd.region as region,
            cd.level_id,
            cd.camera_model_name

        from {{ ref("int_capture_processing") }} cp
        left join
            {{ ref("int_capture_details") }} cd
            on cp.region_capture_id = cd.region_capture_id
    )

select *
from source
