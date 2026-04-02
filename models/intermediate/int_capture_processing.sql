WITH captures AS (
    SELECT 
        tenant,
        region,
        region_capture_id,
        capture_trace_id,
        region_capture_trace_id,
        created_at,
        editing_id,
        cycle_state,
        editor_id,
        error_code,
        reconstruction_error_code,
        level_id,
        refinement_error_code,
        video_length,
        record_id,
        capture_type,
        editor_level,
        editor_work_part,
        region_facility_id,
        region_team_id,
        region_workspace_id,
    FROM {{ ref('int_capture_details') }}
),

facilities AS (
    SELECT 
        f.region_facility_id,
        f.facility_name,
        f.region,
        f.tenant,
        w.workspace_id,
        w.workspace_name,
        t.team_id,
        t.team_name
    FROM {{ ref('stg_tesla__facilities') }} f
    LEFT JOIN {{ ref('stg_tesla__workspaces') }} w
        ON f.region_workspace_id = w.region_workspace_id
        AND f.tenant = w.tenant
    LEFT JOIN {{ ref('stg_tesla__teams') }} t
        ON w.region_team_id = t.region_team_id
        AND w.tenant = t.tenant
),

capture_trace AS (
    SELECT *
    FROM {{ ref('stg_cupixworks__capture_traces') }}
),  

editings AS (
    SELECT *
    FROM {{ ref('stg_tesla__editings') }}
),

users AS (
    SELECT *
    FROM {{ ref('stg_tesla__users') }}
),

editing_started AS (
    SELECT
        capture_trace_id,
        MAX(CASE 
            WHEN stage LIKE '%editing_editing%' 
            THEN timestamp 
        END) AS editing_started_at
    FROM capture_trace
    GROUP BY capture_trace_id
),

re_edit_requested as (
        select
            region_capture_id,
            count(*) as re_edit_count,
            max(created_at) as last_re_edit_requested_at
        from {{ ref("stg_slack__re_edit_requested") }}
        group by 1
    ),

refinement_times AS (
    SELECT
        capture_trace_id,
        MIN(CASE WHEN stage LIKE '%create_refinement%' THEN timestamp END) AS create_refinement_at
    FROM capture_trace
    GROUP BY capture_trace_id
),

editing_created_at AS (
    SELECT
        captures.capture_trace_id,
        COALESCE(
            editings.created_at,
            MIN(CASE WHEN capture_trace.stage LIKE '%editing_ready%' THEN capture_trace.timestamp END)
        ) AS editing_created_at
    FROM captures
    LEFT JOIN editings
        ON editings.editing_id = captures.editing_id
        AND editings.tenant = captures.tenant
        AND editings.region = captures.region
    LEFT JOIN capture_trace
        ON capture_trace.capture_trace_id = captures.capture_trace_id
        AND capture_trace.tenant = captures.tenant
    GROUP BY captures.capture_trace_id, editings.created_at
),

review_finished_cte AS (
    SELECT
        ct.capture_trace_id,
        MIN(ct.timestamp) AS review_finished_at
    FROM capture_trace ct
    INNER JOIN (
        SELECT 
            capture_trace_id,
            MAX(CASE WHEN stage LIKE '%editing_in_review%' THEN timestamp END) AS last_review_started_at
        FROM capture_trace
        GROUP BY capture_trace_id
    ) r ON ct.capture_trace_id = r.capture_trace_id
    WHERE (ct.stage LIKE '%editing_done%' OR ct.stage LIKE '%escalat%')
    AND ct.timestamp > r.last_review_started_at
    GROUP BY ct.capture_trace_id
),

final AS (
    SELECT
        captures.*,
        MIN(CASE WHEN capture_trace.stage LIKE '%uploading_finished%' THEN capture_trace.timestamp END) AS uploading_finished_at,
        
        CASE
            WHEN captures.capture_trace_id IS NULL THEN 'untrusted'
            WHEN captures.created_at < '2024-10-01' THEN 'untrusted'
            ELSE 'trusted'
        END AS capture_trace_trust,

        COUNTIF(capture_trace.stage LIKE '%hold%') > 0 AS is_holded,
        COUNTIF(capture_trace.stage LIKE '%escalat%') > 0 AS is_escalated,
        COUNTIF(capture_trace.stage LIKE '%editing_skipped%') > 0 AS editing_skipped,
        coalesce(re_edit_requested.re_edit_count, 0) > 0 as is_re_edited,
        coalesce(re_edit_requested.re_edit_count, 0) as re_edit_count,
        re_edit_requested.last_re_edit_requested_at,

        countif(capture_trace.stage like '%preprocessor_agent_finished%') as pre_process_count,
        countif(capture_trace.stage like '%skat_master_finished%') as master_process_count,
        countif(capture_trace.stage like '%postprocessor_agent_finished%') as post_process_count,
        countif(capture_trace.stage like '%preview_finished%') as preview_process_count,
        countif(capture_trace.stage like '%editing_editing%') as edit_process_count,
        countif(capture_trace.stage like '%editing_in_review%') as review_process_count,
        countif(capture_trace.stage like '%reconstruction_finished%') as reconstruction_process_count,
        countif(capture_trace.stage like '%preview_finished%') > 0 as is_previewed,
        countif(capture_trace.stage like '%skat_master_finished%') > 1 as is_recalculated,
        countif(capture_trace.stage like '%reconstruction_finished%') > 0 as has_cpc,

        MIN(CASE WHEN capture_trace.stage LIKE '%processing_preprocessor_agent_started%' THEN capture_trace.timestamp END) AS preprocessor_agent_started_at,
        MIN(CASE WHEN capture_trace.stage LIKE '%processing_preprocessor_agent_finished%' THEN capture_trace.timestamp END) AS preprocessor_agent_finished_at,
        MIN(CASE WHEN capture_trace.stage LIKE '%processing_postprocessor_agent_started%' THEN capture_trace.timestamp END) AS postprocessor_agent_started_at,
        MIN(CASE WHEN capture_trace.stage LIKE '%processing_postprocessor_agent_finished%' THEN capture_trace.timestamp END) AS postprocessor_agent_finished_at,
        MIN(CASE WHEN capture_trace.stage LIKE '%preview_finished%' THEN capture_trace.timestamp END) AS preview_finished_at,
        MIN(CASE WHEN capture_trace.stage LIKE '%create_refinement%' THEN capture_trace.timestamp END) AS create_refinement_at,
        MIN(CASE 
            WHEN capture_trace.stage LIKE '%processing_postprocessor_agent_started%' 
            AND capture_trace.timestamp > refinement_times.create_refinement_at
            THEN capture_trace.timestamp 
        END) AS postprocessor_agent_started_at_2nd,
        MIN(CASE 
            WHEN capture_trace.stage LIKE '%processing_postprocessor_agent_finished%' 
            AND capture_trace.timestamp > refinement_times.create_refinement_at
            THEN capture_trace.timestamp 
        END) AS postprocessor_agent_finished_at_2nd,

        COALESCE(editings.created_at, MIN(CASE WHEN capture_trace.stage LIKE '%editing_ready%' THEN capture_trace.timestamp END)) AS editing_created_at,

        MAX(CASE WHEN capture_trace.stage LIKE '%editing_%review%' THEN 1 ELSE 0 END) AS has_review,

        CASE
            WHEN MAX(CASE WHEN capture_trace.stage LIKE '%editing_waiting_for_review%' THEN 1 ELSE 0 END) = 1
            AND MAX(CASE WHEN capture_trace.stage LIKE '%editing_in_review%' THEN 1 ELSE 0 END) = 0
            THEN 'skipped'
            WHEN MAX(CASE WHEN capture_trace.stage LIKE '%editing_in_review%' THEN 1 ELSE 0 END) = 1
            THEN 'reviewed'
            ELSE 'no review'
        END AS review_skipped,

        MAX(CASE 
            WHEN capture_trace.stage LIKE '%editing_editing%' 
            AND capture_trace.timestamp > editing_created_at.editing_created_at
            THEN capture_trace.timestamp 
        END) AS edit_started_at,

        CASE
            WHEN MAX(CASE WHEN capture_trace.stage LIKE '%editing_%review%' THEN 1 ELSE 0 END) = 1
            THEN MIN(CASE 
                    WHEN capture_trace.stage LIKE '%editing_waiting_for_review%' 
                    AND capture_trace.timestamp > editing_started.editing_started_at
                    THEN capture_trace.timestamp 
                END)
            ELSE MIN(CASE 
                    WHEN capture_trace.stage LIKE '%editing_done%' 
                    AND capture_trace.timestamp > editing_started.editing_started_at
                    THEN capture_trace.timestamp 
                END)
        END AS edit_finished_at,

        MAX(CASE WHEN stage LIKE '%editing_in_review%' THEN timestamp END) AS review_started_at,

        MAX(CASE WHEN capture_trace.stage LIKE '%processing_reconstruction_started%' THEN capture_trace.timestamp END) AS reconstruction_started_at,
        MAX(CASE WHEN capture_trace.stage LIKE '%processing_reconstruction_finished%' THEN capture_trace.timestamp END) AS reconstruction_finished_at,

        CASE 
            WHEN MAX(CASE WHEN capture_trace.stage LIKE '%editing_%review%' THEN 1 ELSE 0 END) = 1
            THEN review_finished_cte.review_finished_at
            ELSE NULL
        END AS review_finished_at,

        users.user_email AS editor_email,
        facilities.facility_name,
        facilities.workspace_name,
        facilities.team_name

    FROM captures
    LEFT JOIN capture_trace
        ON capture_trace.capture_trace_id = captures.capture_trace_id
        AND capture_trace.tenant = captures.tenant
    LEFT JOIN editings
        ON editings.editing_id = captures.editing_id
        AND editings.tenant = captures.tenant
        AND editings.region = captures.region
    LEFT JOIN editing_started
        ON editing_started.capture_trace_id = captures.capture_trace_id
    LEFT JOIN users
        ON users.user_id = captures.editor_id
        AND users.tenant = captures.tenant
        AND users.region = captures.region
    LEFT JOIN refinement_times
        ON refinement_times.capture_trace_id = captures.capture_trace_id
    LEFT JOIN facilities
        ON facilities.region_facility_id = captures.region_facility_id
        AND facilities.tenant = captures.tenant
    LEFT JOIN re_edit_requested on captures.region_capture_id = re_edit_requested.region_capture_id
    LEFT JOIN editing_created_at
    ON editing_created_at.capture_trace_id = captures.capture_trace_id
    LEFT JOIN review_finished_cte
    ON review_finished_cte.capture_trace_id = captures.capture_trace_id
    GROUP BY
        captures.tenant,
        captures.region,
        captures.region_capture_id,
        captures.capture_trace_id,
        captures.region_capture_trace_id,
        captures.created_at,
        captures.editing_id,
        captures.editor_id,
        captures.error_code,
        captures.capture_type,
        captures.record_id,
        captures.editor_level,
        captures.editor_work_part,
        captures.reconstruction_error_code,
        captures.refinement_error_code,
        captures.cycle_state,
        captures.region_facility_id,   
        captures.region_workspace_id,  
        captures.region_team_id,
        captures.level_id,
        captures.video_length,
        re_edit_requested.re_edit_count,
        re_edit_requested.last_re_edit_requested_at,
        review_finished_cte.review_finished_at,
        editings.created_at,
        editings.state_updated_at,
        editings.updated_at,
        users.user_email,
        facilities.facility_name,
        facilities.workspace_name,
        facilities.team_name
)

SELECT * FROM final
