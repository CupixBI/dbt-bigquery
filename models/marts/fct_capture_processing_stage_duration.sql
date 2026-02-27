select
    region_capture_id,
    capture_type,
    is_sla_exceeded,
    uploading_finished_at,
    stage_name,
    duration_min
from {{ ref("int_capture_processing") }}
unpivot (
    duration_min for stage_name in (
        first_processing_duration_min,
        first_edit_duration_min,
        first_review_waiting_duration_min,
        review_duration_min
    )
)
where duration_min is not null
and is_sla_exceeded is not null