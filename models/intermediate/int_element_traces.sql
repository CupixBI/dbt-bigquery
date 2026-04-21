WITH element_traces AS (
    SELECT * FROM {{ ref('stg_tesla__element_traces') }}
    WHERE purpose IS NULL OR purpose != 'meta_change'
),

statuses AS (
    SELECT
        region,
        status_id,
        region_status_id,
        tenant,
        status_name AS sqa_status_name,
        created_at AS sqa_created_at,
        updated_at AS sqa_updated_at,
        cycle_state AS sqa_cycle_state,
        is_complete_status AS sqa_is_complete_status,
    FROM {{ ref('stg_tesla__statuses') }}
),

final AS (
    SELECT
        element_traces.*,
        statuses.sqa_status_name,
        statuses.sqa_created_at,
        statuses.sqa_updated_at,
        statuses.sqa_cycle_state,
        statuses.sqa_is_complete_status,
        estimated_statuses.sqa_status_name AS estimated_status_name,
    FROM element_traces
    LEFT JOIN statuses
        ON element_traces.region_status_id = statuses.region_status_id
        AND element_traces.tenant = statuses.tenant
    LEFT JOIN statuses AS estimated_statuses  -- 추가
        ON element_traces.region_estimated_status_id = estimated_statuses.region_status_id
        AND element_traces.tenant = estimated_statuses.tenant
)

SELECT * FROM final
