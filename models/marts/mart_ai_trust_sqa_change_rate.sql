WITH base AS (
    SELECT
        DATE(created_at, 'Asia/Seoul') AS created_date_kst,
        region,
        tenant,
        CONCAT(CAST(element_trace_id AS STRING), '|', region, '|', tenant) AS unique_id,
        COALESCE(CAST(processing_result_trust_level AS STRING), 'Not Evaluated') AS trust_level,
        CASE
            WHEN estimated_status_name IS NOT NULL
             AND sqa_status_name IS NOT NULL
             AND sqa_status_name != estimated_status_name
            THEN TRUE
            ELSE FALSE
        END AS sqa_changed
    FROM {{ ref('int_element_traces') }}
)

SELECT
    created_date_kst,
    region,
    tenant,
    trust_level,
    COUNT(DISTINCT unique_id)                                                             AS total_count,
    COUNT(DISTINCT CASE WHEN sqa_changed THEN unique_id END)                             AS sqa_changed_count,
    COUNT(DISTINCT unique_id) - COUNT(DISTINCT CASE WHEN sqa_changed THEN unique_id END) AS sqa_unchanged_count,
    ROUND(COUNT(DISTINCT CASE WHEN sqa_changed THEN unique_id END)
          / COUNT(DISTINCT unique_id) * 100, 2)                                          AS sqa_change_rate
FROM base
GROUP BY
    created_date_kst,
    region,
    tenant,
    trust_level
ORDER BY
    created_date_kst,
    region,
    tenant,
    trust_level