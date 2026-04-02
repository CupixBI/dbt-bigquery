with
    source as (
        select
            cp.region_capture_id,
            cp.region_capture_trace_id,
            cp.tenant,
            cp.capture_type,
            cp.record_id,
            cp.video_length,
            cp.error_code,
            cp.reconstruction_error_code,
            cp.editor_level,
            cp.editor_work_part,

            -- KST 변환
            datetime(cp.created_at, 'Asia/Seoul') as created_at_kst,
            datetime(cp.uploading_finished_at, 'Asia/Seoul') as uploading_finished_at_kst,
            datetime(cp.edit_started_at, 'Asia/Seoul') as edit_started_at_kst,
            datetime(cp.review_started_at, 'Asia/Seoul') as review_started_at_kst,

            -- timestamps
            cp.uploading_finished_at,
            cp.preprocessor_agent_started_at,
            cp.preprocessor_agent_finished_at,
            cp.postprocessor_agent_started_at,
            cp.postprocessor_agent_finished_at,
            cp.postprocessor_agent_started_at_2nd,
            cp.postprocessor_agent_finished_at_2nd,
            cp.preview_finished_at,
            cp.editing_created_at,
            cp.edit_started_at,
            cp.edit_finished_at,
            cp.review_started_at,
            cp.review_finished_at,
            cp.reconstruction_started_at,
            cp.reconstruction_finished_at,

            -- flags
            cp.is_holded,
            cp.is_escalated,
            cp.has_cpc,
            cp.has_review,
            cp.is_previewed,
            cp.is_recalculated,

            -- process counts
            cp.edit_process_count,
            cp.pre_process_count,
            cp.master_process_count,
            cp.post_process_count,
            cp.preview_process_count,
            cp.review_process_count,
            cp.reconstruction_process_count,
            cp.re_edit_count,

            -- duration 계산
            TIMESTAMP_DIFF(cp.postprocessor_agent_finished_at, cp.uploading_finished_at, MINUTE) as uploading_to_processing_finished_min,
            TIMESTAMP_DIFF(cp.preview_finished_at, cp.postprocessor_agent_finished_at, MINUTE) as processing_finished_to_preview_finished_min,
            TIMESTAMP_DIFF(cp.edit_started_at, cp.preview_finished_at, MINUTE) as preview_finished_to_edit_started_min,
            TIMESTAMP_DIFF(cp.edit_started_at, cp.postprocessor_agent_finished_at, MINUTE) as post_finished_to_edit_started_min,
            TIMESTAMP_DIFF(cp.edit_finished_at, cp.edit_started_at, MINUTE) as edit_started_to_edit_finished_min,
            TIMESTAMP_DIFF(cp.review_started_at, cp.edit_finished_at, MINUTE) as edit_finished_to_review_started_min,
            TIMESTAMP_DIFF(cp.review_finished_at, cp.review_started_at, MINUTE) as review_started_to_review_finished_min,
            TIMESTAMP_DIFF(cp.reconstruction_finished_at, cp.reconstruction_started_at, MINUTE) as first_cpc_generation_duration_min,

            -- total lead time
            TIMESTAMP_DIFF(
                CASE
                    WHEN cp.capture_type = '3D Map' THEN cp.reconstruction_finished_at
                    WHEN cp.has_review = 1 THEN cp.review_finished_at
                    ELSE cp.edit_finished_at
                END,
                cp.uploading_finished_at,
                MINUTE
            ) as total_lead_time_min,

            -- is_sla_exceeded (8시간 = 480분)
            TIMESTAMP_DIFF(
                CASE
                    WHEN cp.capture_type = '3D Map' THEN cp.reconstruction_finished_at
                    WHEN cp.has_review = 1 THEN cp.review_finished_at
                    ELSE cp.edit_finished_at
                END,
                cp.uploading_finished_at,
                MINUTE
            ) > 480 as is_sla_exceeded,

            -- video_length_range
            CASE
                WHEN cp.video_length < 300 THEN '~5min'
                WHEN cp.video_length < 600 THEN '5~10min'
                WHEN cp.video_length < 1200 THEN '10~20min'
                ELSE '20min~'
            END as video_length_range,

            -- is_last_week
            date_trunc(cast(cp.uploading_finished_at as date), week(monday))
                = date_trunc(date_sub(current_date(), interval 1 week), week(monday)) as is_last_week,

            -- 메타데이터
            cd.team_name,
            cd.region_team_id,
            cd.facility_name as project_name,
            cd.region_facility_id as project_id,
            cd.captured_by_user_email as creator,
            cd.editor_email,
            cd.region,
            cd.level_id,
            cd.camera_model_name

        from {{ ref("int_capture_processing") }} cp
        left join {{ ref("int_capture_details") }} cd
            on cp.region_capture_id = cd.region_capture_id
            and cp.tenant = cd.tenant
        where
            cp.pre_process_count > 0
            and cp.cycle_state = 'created'
            and NOT (cp.capture_type IN ('3D Map', 'Video') AND cp.video_length = 0)
    )

select *
from source