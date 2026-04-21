WITH created AS (
    SELECT
        DATE(created_at) AS date,
        DATE(created_at, 'Asia/Seoul') AS date_kst,
        region,
        tenant,
        COALESCE(estimated_status_name, 'Not Evaluated') AS status_name,
        COALESCE(CAST(processing_result_trust_level AS STRING), 'Not Evaluated') AS trust_level,
        COUNT(*) AS element_trace_count,
        'created' AS type
    FROM {{ ref('int_element_traces') }}
    GROUP BY 1, 2, 3, 4, 5, 6
),

sqa_processed AS (
    SELECT
        DATE(updated_at) AS date,
        DATE(updated_at, 'Asia/Seoul') AS date_kst,
        region,
        tenant,
        COALESCE(sqa_status_name, 'Not Evaluated') AS status_name,
        COALESCE(CAST(processing_result_trust_level AS STRING), 'Not Evaluated') AS trust_level,
        COUNT(*) AS element_trace_count,
        'sqa_processed' AS type
    FROM {{ ref('int_element_traces') }}
    WHERE estimated_status_name IS NOT NULL
      AND sqa_status_name != estimated_status_name
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT * FROM created
UNION ALL
SELECT * FROM sqa_processed
ORDER BY date, region, tenant, type, status_name
