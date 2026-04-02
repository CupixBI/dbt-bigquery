with base as (
    select
        region_capture_id,
        uploading_finished_at,
        video_length_range,
        is_sla_exceeded,
        capture_type,
        team_name,
        region_team_id,
        project_name,
        uploading_to_processing_finished_min,
        processing_finished_to_preview_finished_min,
        preview_finished_to_edit_started_min,
        edit_started_to_edit_finished_min,
        edit_finished_to_review_started_min,
        review_started_to_review_finished_min,
        first_cpc_generation_duration_min
    from {{ ref('fct_capture_processing_enriched') }}
)

select uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '1. Upload → Processing' as stage, uploading_to_processing_finished_min as duration_min from base
union all
select uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '2. Processing → Preview' as stage, processing_finished_to_preview_finished_min as duration_min from base
union all
select uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '3. Preview → Edit Wait' as stage, preview_finished_to_edit_started_min as duration_min from base
union all
select uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '4. Edit Duration' as stage, edit_started_to_edit_finished_min as duration_min from base
union all
select uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '5. Edit → Review Wait' as stage, edit_finished_to_review_started_min as duration_min from base
union all
select uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '6. Review Duration' as stage, review_started_to_review_finished_min as duration_min from base
union all
select uploading_finished_at, is_sla_exceeded, capture_type, team_name, region_team_id, project_name, video_length_range, '7. CPC Generation' as stage, first_cpc_generation_duration_min as duration_min from base

order by
  case stage
    when '1. Upload → Processing' then 1
    when '2. Processing → Preview' then 2
    when '3. Preview → Edit Wait' then 3
    when '4. Edit Duration' then 4
    when '5. Edit → Review Wait' then 5
    when '6. Review Duration' then 6
    when '7. CPC Generation' then 7
  end,
  case video_length_range
    when '~5min' then 1
    when '5~10min' then 2
    when '10~20min' then 3
    when '20min~' then 4
  end