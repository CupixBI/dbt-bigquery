select
    editor_email,
    date_trunc(uploading_finished_at, week(monday)) as week,
    count(distinct date(uploading_finished_at)) as active_days,
    count(distinct region_capture_id) as capture_count,
    count(distinct date(uploading_finished_at)) / 5.0 as fte,
    countif(is_sla_exceeded = true) as sla_exceeded_count,
    countif(is_sla_exceeded = true) / count(distinct region_capture_id) * 100 as sla_exceeded_pct
from {{ ref('fct_capture_processing_enriched') }}
group by 1, 2