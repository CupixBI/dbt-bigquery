with base as (
    select
        region_capture_id,
        video_length_range,
        uploading_to_processing_finished_min,
        processing_finished_to_preview_finished_min,
        preview_finished_to_edit_started_min,
        edit_started_to_edit_finished_min,
        edit_finished_to_review_started_min,
        review_started_to_review_finished_min,
        first_cpc_generation_duration_min
    from {{ ref('int_capture_processing') }}
)

select
    video_length_range,
    'uploading_to_processing_finished' as stage,
    uploading_to_processing_finished_min as duration_min
from base
union all
select
    video_length_range,
    'processing_finished_to_preview_finished' as stage,
    processing_finished_to_preview_finished_min as duration_min
from base
union all
select
    video_length_range,
    'preview_finished_to_edit_started' as stage,
    preview_finished_to_edit_started_min as duration_min
from base
union all
select
    video_length_range,
    'edit_started_to_edit_finished' as stage,
    edit_started_to_edit_finished_min as duration_min
from base
union all
select
    video_length_range,
    'edit_finished_to_review_started' as stage,
    edit_finished_to_review_started_min as duration_min
from base
union all
select
    video_length_range,
    'review_started_to_review_finished' as stage,
    review_started_to_review_finished_min as duration_min
from base
union all
select
    video_length_range,
    'first_cpc_generation_duration' as stage,
    first_cpc_generation_duration_min as duration_min
from base

-- ✅ 두 축 모두 순서 지정
order by
  case stage
    when 'uploading_to_processing_finished' then 1
    when 'processing_finished_to_preview_finished' then 2
    when 'preview_finished_to_edit_started' then 3
    when 'edit_started_to_edit_finished' then 4
    when 'edit_finished_to_review_started' then 5
    when 'review_started_to_review_finished' then 6
    when 'first_cpc_generation_duration' then 7
  end,
  case video_length_range
    when 'Under 6 min' then 1
    when '6–12 min' then 2
    when '12–18 min' then 3
    when 'Over 18 min' then 4
  end