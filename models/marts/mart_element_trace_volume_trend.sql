WITH base AS (
    SELECT
        DATE(created_at) AS created_date,
        DATE(created_at, 'Asia/Seoul') AS created_date_kst,
        DATE(updated_at) AS updated_date,
        DATE(updated_at, 'Asia/Seoul') AS updated_date_kst,
        region,
        tenant,
        COALESCE(CAST(processing_result_trust_level AS STRING), 'Not Evaluated') AS trust_level,
        COALESCE(estimated_status_name, 'Not Evaluated') AS estimated_status_name,
        COALESCE(sqa_status_name, 'Not Evaluated') AS sqa_status_name,
    FROM {{ ref('int_element_traces') }}
),

final AS (
    SELECT
        created_date,
        created_date_kst,
        updated_date,
        updated_date_kst,
        region,
        tenant,
        trust_level,
        estimated_status_name,
        sqa_status_name,
        COUNT(*) AS element_trace_count,
    FROM base
    GROUP BY created_date, created_date_kst, updated_date, updated_date_kst, region, tenant, trust_level, estimated_status_name, sqa_status_name
)

SELECT * FROM final
ORDER BY created_date, created_date_kst, updated_date, updated_date_kst, region, tenant, trust_level
