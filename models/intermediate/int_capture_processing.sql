with
    captures as (
        select region_capture_id, capture_trace_id, video_length, created_at, error_code, reconstruction_error_code, capture_type, cycle_state, editor_email, editor_level, editor_work_part
        from {{ ref("int_capture_details") }}
    ),

    capture_traces as (
        select capture_trace_id, region_capture_trace_id, stage, timestamp, timestamp_kst  -- ✅ timestamp_kst 추가
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
    select ct.capture_trace_id, min(ct.timestamp_kst) as first_edit_end_at  -- ✅
    from {{ ref("stg_cupixworks__capture_traces") }} ct
    inner join (
        select capture_trace_id, min(timestamp_kst) as editing_started_at  -- ✅
        from {{ ref("stg_cupixworks__capture_traces") }}
        where stage like '%editing_editing%'
        group by capture_trace_id
    ) first_edit
        on ct.capture_trace_id = first_edit.capture_trace_id
        and ct.timestamp_kst > first_edit.editing_started_at  -- ✅
        and (
            ct.stage like '%editing_done%'
            or ct.stage like '%editing_in_review%'
            or ct.stage like '%editing_waiting_for_review%'
            or ct.stage like '%editing_holding%'
            or ct.stage like '%editing_escalating%'
        )
    group by ct.capture_trace_id
),

    site_view_published AS (
    -- 3d_map: 구간 내 editing_done or reconstruction_started
    SELECT
        ct.capture_trace_id,
        MIN(ct.timestamp_kst) AS site_view_published_at
    FROM {{ ref("stg_cupixworks__capture_traces") }} ct
    INNER JOIN captures c
        ON ct.capture_trace_id = c.capture_trace_id
    -- 리뷰 여부 & 구간 기준점 계산
    INNER JOIN (
        SELECT
            capture_trace_id,
            MIN(CASE WHEN stage LIKE '%editing_in_review%' THEN timestamp_kst END) AS review_started_at,
            MIN(CASE WHEN stage LIKE '%editing_editing%'   THEN timestamp_kst END) AS editing_started_at,
            MIN(CASE WHEN stage LIKE '%reconstruction_started%' THEN timestamp_kst END) AS reconstruction_started_at,
            -- 구간 기준: 리뷰 있으면 editing_in_review, 없으면 editing_editing
            COALESCE(
                MIN(CASE WHEN stage LIKE '%editing_in_review%' THEN timestamp_kst END),
                MIN(CASE WHEN stage LIKE '%editing_editing%'   THEN timestamp_kst END)
            ) AS range_start
        FROM {{ ref("stg_cupixworks__capture_traces") }}
        GROUP BY capture_trace_id
    ) bounds
        ON ct.capture_trace_id = bounds.capture_trace_id
    -- editing_done in range 여부
    LEFT JOIN (
        SELECT
            ed.capture_trace_id,
            MIN(ed.timestamp_kst) AS first_editing_done_in_range
        FROM {{ ref("stg_cupixworks__capture_traces") }} ed
        INNER JOIN (
            SELECT
                capture_trace_id,
                COALESCE(
                    MIN(CASE WHEN stage LIKE '%editing_in_review%' THEN timestamp_kst END),
                    MIN(CASE WHEN stage LIKE '%editing_editing%'   THEN timestamp_kst END)
                ) AS range_start,
                MIN(CASE WHEN stage LIKE '%reconstruction_started%' THEN timestamp_kst END) AS reconstruction_started_at
            FROM {{ ref("stg_cupixworks__capture_traces") }}
            GROUP BY capture_trace_id
        ) r ON ed.capture_trace_id = r.capture_trace_id
        WHERE ed.stage LIKE '%editing_done%'
          AND ed.timestamp_kst > r.range_start
          AND ed.timestamp_kst < r.reconstruction_started_at
        GROUP BY ed.capture_trace_id
    ) done_in_range
        ON ct.capture_trace_id = done_in_range.capture_trace_id
    WHERE c.capture_type = '3d_map'
      AND (
          -- editing_done in range 있으면 그거
          (ct.stage LIKE '%editing_done%' AND ct.timestamp_kst = done_in_range.first_editing_done_in_range)
          -- 없으면 reconstruction_started
          OR (done_in_range.first_editing_done_in_range IS NULL AND ct.stage LIKE '%reconstruction_started%')
      )
    GROUP BY ct.capture_trace_id

    UNION ALL

    -- 3d_map 아닌 경우: max(editing_done)
    SELECT
        ct.capture_trace_id,
        MAX(ct.timestamp_kst) AS site_view_published_at
    FROM {{ ref("stg_cupixworks__capture_traces") }} ct
    INNER JOIN captures c
        ON ct.capture_trace_id = c.capture_trace_id
    WHERE c.capture_type != '3d_map'
      AND ct.stage LIKE '%editing_done%'
    GROUP BY ct.capture_trace_id
),

    review_duration as (
        select
            e.capture_trace_id,
            timestamp_diff(
                coalesce(
                    min(case when d.stage like '%editing_done%' then d.timestamp_kst end),  -- ✅
                    min(case when d.stage like '%reconstruction_started%' then d.timestamp_kst end)  -- ✅
                ),
                min(e.timestamp_kst),  -- ✅
                minute
            ) as review_started_to_review_finished_min
        from {{ ref("stg_cupixworks__capture_traces") }} e
        inner join {{ ref("stg_cupixworks__capture_traces") }} d
            on e.capture_trace_id = d.capture_trace_id
            and d.timestamp_kst > e.timestamp_kst  -- ✅
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
            TIMESTAMP_ADD(captures.created_at, INTERVAL 9 HOUR) AS created_at_kst,
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
            countif(capture_traces.stage like '%skat_master_finished%') > 1 as is_recalculated,
            countif(capture_traces.stage like '%reconstruction_finished%') > 0 as has_cpc,
            countif(capture_traces.stage like '%hold%') > 0 as is_holded,
            countif(capture_traces.stage like '%escalat%') > 0 as is_escalated,
            coalesce(re_edit_requested.re_edit_count, 0) > 0 as is_re_edited,
            coalesce(re_edit_requested.re_edit_count, 0) as re_edit_count,
            re_edit_requested.last_re_edit_requested_at,

            -- timestamp (KST 기준) ✅
            min(case when capture_traces.stage like '%uploading_finished%' then capture_traces.timestamp_kst end) as uploading_finished_at,
            min(case when capture_traces.stage like '%preprocessor_agent_started%' then capture_traces.timestamp_kst end) as preprocessor_agent_started_at,
            min(case when capture_traces.stage like '%preprocessor_agent_finished%' then capture_traces.timestamp_kst end) as preprocessor_agent_finished_at,
            min(case when capture_traces.stage like '%skat_master_started%' then capture_traces.timestamp_kst end) as skat_master_started_at,
            min(case when capture_traces.stage like '%skat_master_finished%' then capture_traces.timestamp_kst end) as skat_master_finished_at,
            min(case when capture_traces.stage like '%postprocessor_agent_started%' then capture_traces.timestamp_kst end) as postprocessor_agent_started_at,
            min(case when capture_traces.stage like '%postprocessor_agent_finished%' then capture_traces.timestamp_kst end) as postprocessor_agent_finished_at,
            min(case when capture_traces.stage like '%preview_finished%' then capture_traces.timestamp_kst end) as preview_finished_at,
            min(case when capture_traces.stage like '%editing_editing%' then capture_traces.timestamp_kst end) as edit_started_at,
            first_edit_end.first_edit_end_at as edit_finished_at,
            min(case when capture_traces.stage like '%editing_in_review%' then capture_traces.timestamp_kst end) as review_started_at,
            site_view_published.site_view_published_at,
            min(case when capture_traces.stage like '%reconstruction_started%' then capture_traces.timestamp_kst end) as reconstruction_started_at,
            min(case when capture_traces.stage like '%reconstruction_finished%' then capture_traces.timestamp_kst end) as reconstruction_finished_at,

            -- 소요 시간 (단위: 분)
            timestamp_diff(
                min(case when capture_traces.stage like '%postprocessor_agent_finished%' then capture_traces.timestamp_kst end),
                min(case when capture_traces.stage like '%uploading_finished%' then capture_traces.timestamp_kst end),
                minute
            ) as uploading_to_processing_finished_min,

            timestamp_diff(
                min(case when capture_traces.stage like '%preview_finished%' then capture_traces.timestamp_kst end),
                min(case when capture_traces.stage like '%postprocessor_agent_started%' then capture_traces.timestamp_kst end),
                minute
            ) as processing_finished_to_preview_finished_min,

            timestamp_diff(
                min(case when capture_traces.stage like '%editing_editing%' then capture_traces.timestamp_kst end),
                min(case when capture_traces.stage like '%preview_finished%' then capture_traces.timestamp_kst end),
                minute
            ) as preview_finished_to_edit_started_min,

            timestamp_diff(
                first_edit_end.first_edit_end_at,
                min(case when capture_traces.stage like '%editing_editing%' then capture_traces.timestamp_kst end),
                minute
            ) as edit_started_to_edit_finished_min,

            timestamp_diff(
                min(case when capture_traces.stage like '%editing_in_review%' then capture_traces.timestamp_kst end),
                min(case when capture_traces.stage like '%editing_waiting_for_review%' then capture_traces.timestamp_kst end),
                minute
            ) as edit_finished_to_review_started_min,

            review_duration.review_started_to_review_finished_min,

            timestamp_diff(
                min(case when capture_traces.stage like '%reconstruction_finished%' then capture_traces.timestamp_kst end),
                min(case when capture_traces.stage like '%reconstruction_started%' then capture_traces.timestamp_kst end),
                minute
            ) as first_cpc_generation_duration_min,

            -- SLA
            timestamp_diff(
                site_view_published.site_view_published_at,
                min(case when capture_traces.stage like '%uploading_finished%' then capture_traces.timestamp_kst end),
                minute
            ) as sv_delivery_lead_time_min,

            timestamp_diff(
                min(case when capture_traces.stage like '%reconstruction_finished%' then capture_traces.timestamp_kst end),
                min(case when capture_traces.stage like '%uploading_finished%' then capture_traces.timestamp_kst end),
                minute
            ) as cpc_delivery_lead_time_min,

            timestamp_diff(
                case
                    when min(case when capture_traces.stage like '%reconstruction_finished%' then capture_traces.timestamp_kst end) is not null
                    then max(case when capture_traces.stage like '%reconstruction_finished%' then capture_traces.timestamp_kst end)
                    else site_view_published.site_view_published_at
                end,
                min(case when capture_traces.stage like '%uploading_finished%' then capture_traces.timestamp_kst end),
                minute
            ) as total_lead_time_min,

            captures.error_code,
            captures.reconstruction_error_code,

            -- SLA 기준값
            case
                when captures.capture_type = '3d_map' then {{ var('sla_3d_map_min') }}
                else {{ var('sla_default_min') }}
            end as sla_threshold_min,
            captures.cycle_state,

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
            review_duration.review_started_to_review_finished_min,
            captures.error_code,
            captures.reconstruction_error_code,
            captures.cycle_state
    ),

    filtered as (
        select *,
            case
                when capture_type = '3d_map' then cpc_delivery_lead_time_min > {{ var('sla_3d_map_min') }}
                else sv_delivery_lead_time_min > {{ var('sla_default_min') }}
            end as is_sla_exceeded,
            format_date('%a', date(uploading_finished_at)) as day_of_week,
            extract(hour from uploading_finished_at) as hour_of_day
        from final
        where uploading_finished_at is not null
        and pre_process_count > 0
        and cycle_state = 'created'
        and NOT (capture_type IN ('3D Map', 'Video') AND video_length = 0)
    )

select
    *,
    case
        when video_length = 0 then '0 min'
        when video_length < 360 then 'Under 6 min'
        when video_length < 720 then '6–12 min'
        when video_length < 1080 then '12–18 min'
        else 'Over 18 min'
    end as video_length_range
from filtered

-- cupix내부 직원이 생성한 캡처 필터링하기