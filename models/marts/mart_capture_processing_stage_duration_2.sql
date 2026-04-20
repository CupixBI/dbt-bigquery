with base as (
    select
        region_capture_id,
        record_id,
        video_length,
        uploading_finished_at,
        video_length_range,
        is_sla_exceeded,
        capture_type,
        team_name,
        region_team_id,
        project_name,
        postprocessor_agent_finished_at,
        preview_finished_at,
        edit_started_at,
        edit_finished_at,
        review_started_at,
        review_finished_at,
        reconstruction_started_at,
        reconstruction_finished_at
    from {{ ref('fct_capture_processing_enriched') }}
)

select region_capture_id, record_id, video_length, uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '1. Upload → Processing' as stage, TIMESTAMP_DIFF(postprocessor_agent_finished_at, uploading_finished_at, SECOND) as duration_sec from base
union all
select region_capture_id, record_id, video_length, uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '2. Processing → Preview' as stage, TIMESTAMP_DIFF(preview_finished_at, postprocessor_agent_finished_at, SECOND) as duration_sec from base
union all
select region_capture_id, record_id, video_length, uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '3. Preview → Edit Wait' as stage, TIMESTAMP_DIFF(edit_started_at, preview_finished_at, SECOND) as duration_sec from base
union all
select region_capture_id, record_id, video_length, uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '4. Edit Duration' as stage, TIMESTAMP_DIFF(edit_finished_at, edit_started_at, SECOND) as duration_sec from base
union all
select region_capture_id, record_id, video_length, uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '5. Edit → Review Wait' as stage, TIMESTAMP_DIFF(review_started_at, edit_finished_at, SECOND) as duration_sec from base
union all
select region_capture_id, record_id, video_length, uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '6. Review Duration' as stage, TIMESTAMP_DIFF(review_finished_at, review_started_at, SECOND) as duration_sec from base
union all
select region_capture_id, record_id, video_length, uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '7. CPC Wait' as stage, TIMESTAMP_DIFF(reconstruction_started_at, review_finished_at, SECOND) as duration_sec from base
union all
select region_capture_id, record_id, video_length, uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '8. CPC Generation' as stage, TIMESTAMP_DIFF(reconstruction_finished_at, reconstruction_started_at, SECOND) as duration_sec from base

order by
  case stage
    when '1. Upload → Processing' then 1
    when '2. Processing → Preview' then 2
    when '3. Preview → Edit Wait' then 3
    when '4. Edit Duration' then 4
    when '5. Edit → Review Wait' then 5
    when '6. Review Duration' then 6
    when '7. CPC Wait' then 7
    when '8. CPC Generation' then 8
  end,
  case video_length_range
    when '~5min' then 1
    when '5~10min' then 2
    when '10~20min' then 3
    when '20min~' then 4
  end
