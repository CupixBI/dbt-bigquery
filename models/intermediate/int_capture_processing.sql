with
    captures as (
        select region_capture_id, capture_trace_id, video_length, created_at, error_code, reconstruction_error_code, capture_type 
        from {{ ref("stg_tesla__captures") }}
    ),

    capture_traces as (
        select capture_trace_id, region_capture_trace_id, stage, timestamp
        from {{ ref("stg_cupixworks__capture_traces") }}
    ),

    re_edit_requested as (
        select
            region_capture_id,
            count(*) as re_edit_count,
            max(created_at) as last_re_edit_requested_at
        from {{ ref("stg_slack__re_edit_requested") }}
        group by 1
    ),

    first_edit_end as (
    select ct.capture_trace_id, min(ct.timestamp) as first_edit_end_at
    from {{ ref("stg_cupixworks__capture_traces") }} ct
    inner join (
        select capture_trace_id, min(timestamp) as editing_started_at
        from {{ ref("stg_cupixworks__capture_traces") }}
        where stage like '%editing_editing%'
        group by capture_trace_id
    ) first_edit
        on ct.capture_trace_id = first_edit.capture_trace_id
        and ct.timestamp > first_edit.editing_started_at
        and (
            ct.stage like '%editing_done%'
            or ct.stage like '%editing_in_review%'
            or ct.stage like '%editing_waiting_for_review%'
            or ct.stage like '%editing_holding%'
            or ct.stage like '%editing_escalating%'
        )
    group by ct.capture_trace_id
),

    site_view_published as (
        select ct.capture_trace_id, min(ct.timestamp) as site_view_published_at
        from {{ ref("stg_cupixworks__capture_traces") }} ct
        inner join captures c
            on ct.capture_trace_id = c.capture_trace_id
        inner join (
            select
                e.capture_trace_id,
                max(case when w.stage like '%editing_waiting_for_review%' then w.timestamp end) as review_time,
                min(e.timestamp) as editing_time
            from {{ ref("stg_cupixworks__capture_traces") }} e
            left join {{ ref("stg_cupixworks__capture_traces") }} w
                on e.capture_trace_id = w.capture_trace_id
                and w.stage like '%editing_waiting_for_review%'
            where e.stage like '%editing_editing%'
            group by e.capture_trace_id
        ) base
            on ct.capture_trace_id = base.capture_trace_id
            and ct.timestamp > coalesce(base.review_time, base.editing_time)
            and (
                -- 리뷰 있고 3d_map이면 reconstruction_started
                (base.review_time is not null and c.capture_type = '3d_map' and ct.stage like '%reconstruction_started%')
                -- 리뷰 있고 3d_map 아니면 editing_done
                or (base.review_time is not null and c.capture_type != '3d_map' and ct.stage like '%editing_done%')
                -- 리뷰 없으면 editing_done
                or (base.review_time is null and ct.stage like '%editing_done%')
            )
        group by ct.capture_trace_id
    ),

    review_duration as (
        select
            e.capture_trace_id,
            timestamp_diff(
                coalesce(
                    min(case when d.stage like '%editing_done%' then d.timestamp end),
                    min(case when d.stage like '%reconstruction_started%' then d.timestamp end)
                ),
                min(e.timestamp),
                minute
            ) as review_duration_min
        from {{ ref("stg_cupixworks__capture_traces") }} e
        inner join {{ ref("stg_cupixworks__capture_traces") }} d
            on e.capture_trace_id = d.capture_trace_id
            and d.timestamp > e.timestamp
            and (
                d.stage like '%editing_done%'
                or d.stage like '%reconstruction_started%'
            )
        where e.stage like '%editing_in_review%'
        group by e.capture_trace_id
    ),

    final as (
        select
            captures.region_capture_id,
            captures.capture_type,
            max(capture_traces.region_capture_trace_id) as region_capture_trace_id,
            captures.created_at,
            captures.video_length,

            -- 플래그
            countif(capture_traces.stage like '%preprocessor_agent_finished%') as pre_process_count,
            countif(capture_traces.stage like '%skat_master_finished%') as master_process_count,
            countif(capture_traces.stage like '%postprocessor_agent_finished%') as post_process_count,
            countif(capture_traces.stage like '%preview_finished%') as preview_process_count,
            countif(capture_traces.stage like '%editing_editing%') as edit_process_count,
            countif(capture_traces.stage like '%editing_in_review%') as review_process_count,
            countif(capture_traces.stage like '%reconstruction_finished%') as reconstruction_process_count,
            countif(capture_traces.stage like '%preview_finished%') > 0 as is_previewed,
            countif(capture_traces.stage like '%skat_master_finished%') > 0 as is_recalculated,
            countif(capture_traces.stage like '%reconstruction_finished%') > 0 as has_cpc,
            coalesce(re_edit_requested.re_edit_count, 0) > 0 as is_re_edited,
            coalesce(re_edit_requested.re_edit_count, 0) as re_edit_count,
            re_edit_requested.last_re_edit_requested_at,

            -- timestamp
            min(case when capture_traces.stage like '%uploading_finished%' then capture_traces.timestamp end) as uploading_finished_at,
            min(case when capture_traces.stage like '%preprocessor_agent_started%' then capture_traces.timestamp end) as preprocessor_agent_started_at,
            min(case when capture_traces.stage like '%preprocessor_agent_finished%' then capture_traces.timestamp end) as preprocessor_agent_finished_at,
            min(case when capture_traces.stage like '%skat_master_started%' then capture_traces.timestamp end) as skat_master_started_at,
            min(case when capture_traces.stage like '%skat_master_finished%' then capture_traces.timestamp end) as skat_master_finished_at,
            min(case when capture_traces.stage like '%postprocessor_agent_started%' then capture_traces.timestamp end) as postprocessor_agent_started_at,
            min(case when capture_traces.stage like '%postprocessor_agent_finished%' then capture_traces.timestamp end) as postprocessor_agent_finished_at,
            min(case when capture_traces.stage like '%preview_finished%' then capture_traces.timestamp end) as preview_finished_at,
            min(case when capture_traces.stage like '%editing_editing%' then capture_traces.timestamp end) as edit_started_at,
            first_edit_end.first_edit_end_at as edit_finished_at,
            min(case when capture_traces.stage like '%editing_in_review%' then capture_traces.timestamp end) as review_started_at,
            site_view_published.site_view_published_at,
            min(case when capture_traces.stage like '%reconstruction_started%' then capture_traces.timestamp end) as reconstruction_started_at,
            min(case when capture_traces.stage like '%reconstruction_finished%' then capture_traces.timestamp end) as reconstruction_finished_at,

            -- 소요 시간 (단위: 분)
            timestamp_diff(
                min(case when capture_traces.stage like '%postprocessor_agent_finished%' then capture_traces.timestamp end),
                min(case when capture_traces.stage like '%uploading_finished%' then capture_traces.timestamp end),
                minute
            ) as uploading_to_processing_finished_min,

            timestamp_diff(
                min(case when capture_traces.stage like '%preview_finished%' then capture_traces.timestamp end),
                min(case when capture_traces.stage like '%postprocessor_agent_started%' then capture_traces.timestamp end),
                minute
            ) as processing_finished_to_preview_finished_min,

            timestamp_diff(
                min(case when capture_traces.stage like '%preview_finished%' then capture_traces.timestamp end),
                min(case when capture_traces.stage like '%editing_editing%' then capture_traces.timestamp end),
                minute
            ) as first_edit_wating_duration_min,

            timestamp_diff(
                first_edit_end.first_edit_end_at,
                min(case when capture_traces.stage like '%editing_editing%' then capture_traces.timestamp end),
                minute
            ) as first_edit_duration_min,

            timestamp_diff(
                min(case when capture_traces.stage like '%editing_in_review%' then capture_traces.timestamp end),
                min(case when capture_traces.stage like '%editing_waiting_for_review%' then capture_traces.timestamp end),
                minute
            ) as first_review_waiting_duration_min,

            review_duration.review_duration_min,

            -- SLA
            timestamp_diff(
                site_view_published.site_view_published_at,
                min(case when capture_traces.stage like '%uploading_finished%' then capture_traces.timestamp end),
                minute
            ) as sv_delivery_lead_time_min,

            timestamp_diff(
                min(case when capture_traces.stage like '%reconstruction_finished%' then capture_traces.timestamp end),
                min(case when capture_traces.stage like '%uploading_finished%' then capture_traces.timestamp end),
                minute
            ) as cpc_delivery_lead_time_min,

            timestamp_diff(
                case
                    when min(case when capture_traces.stage like '%reconstruction_finished%' then capture_traces.timestamp end) is not null
                    then max(case when capture_traces.stage like '%reconstruction_finished%' then capture_traces.timestamp end)
                    else site_view_published.site_view_published_at
                end,
                min(case when capture_traces.stage like '%uploading_finished%' then capture_traces.timestamp end),
                minute
            ) as total_lead_time_min,

            captures.error_code,
            captures.reconstruction_error_code,

            -- SLA 기준값
            case
                when captures.capture_type = '3d_map' then 480
                else 360
            end as sla_threshold_min

        from captures
        left join capture_traces on capture_traces.capture_trace_id = captures.capture_trace_id
        left join re_edit_requested on captures.region_capture_id = re_edit_requested.region_capture_id
        left join first_edit_end on captures.capture_trace_id = first_edit_end.capture_trace_id
        left join site_view_published on captures.capture_trace_id = site_view_published.capture_trace_id
        left join review_duration on captures.capture_trace_id = review_duration.capture_trace_id
        group by
            captures.region_capture_id,
            captures.capture_type,
            captures.created_at,
            captures.video_length,
            re_edit_requested.re_edit_count,
            re_edit_requested.last_re_edit_requested_at,
            first_edit_end.first_edit_end_at,
            site_view_published.site_view_published_at,
            review_duration.review_duration_min,
            captures.error_code,
            captures.reconstruction_error_code
    ),

    filtered as (
        select *,
            case
                when capture_type = '3d_map' then cpc_delivery_lead_time_min > 480
                else sv_delivery_lead_time_min > 360
            end as is_sla_exceeded,
            format_date('%a', date(uploading_finished_at)) as day_of_week,
            extract(hour from uploading_finished_at) as hour_of_day
        from final
        where uploading_finished_at is not null
        and pre_process_count > 0
    )

select *
from filtered