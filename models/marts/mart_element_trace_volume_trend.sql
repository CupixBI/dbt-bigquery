WITH base AS (
    SELECT
        DATE(created_at) AS created_date,
        DATE(created_at, 'Asia/Seoul') AS created_date_kst,
        region,
        tenant,
        COALESCE(CAST(processing_result_trust_level AS STRING), 'Not Evaluated') AS trust_level,
    FROM {{ ref('int_element_traces') }}
),

final AS (
    SELECT
        created_date,
        created_date_kst,
        region,
        tenant,
        trust_level,
        COUNT(*) AS element_trace_count,
    FROM base
    GROUP BY created_date, created_date_kst, region, tenant, trust_level
)

SELECT * FROM final
ORDER BY created_date,created_date_kst, region, tenant, trust_level
