WITH capture_traces AS (
  SELECT
    id AS capture_trace_id,
    log_json,
    region,
    tenant,
  FROM {{ source('cupixworks', 'capture_traces') }}
),

filtered AS (
  SELECT *
  FROM capture_traces
  WHERE JSON_VALUE(log_json, '$.remark') IS NOT NULL
),

deduped AS (
  SELECT *
  FROM filtered
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      capture_trace_id,
      JSON_VALUE(log_json, '$.remark'),
      JSON_VALUE(log_json, '$."@timestamp"')
    ORDER BY capture_trace_id
  ) = 1
),

final AS (
  SELECT
    capture_trace_id,
    region,
    CONCAT(
            CASE region 
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-',
            capture_trace_id,
            '-',
            TRIM(REPLACE(tenant, '"', ''))
        ) AS region_capture_trace_id,
    TIMESTAMP(JSON_VALUE(log_json, '$."@timestamp"')) AS timestamp,
    TIMESTAMP_ADD(
      TIMESTAMP(JSON_VALUE(log_json, '$."@timestamp"')), 
      INTERVAL 9 HOUR
    ) AS timestamp_kst,
    JSON_VALUE(log_json, '$.remark')                  AS stage,
    JSON_VALUE(log_json, '$.class_name')              AS class_name,
    JSON_VALUE(log_json, '$.class')                   AS class,
    CAST(JSON_VALUE(log_json, '$.model_id') AS STRING)                        AS editing_id,
    REGEXP_EXTRACT(JSON_VALUE(log_json, '$.message'), r'editor_name: ([^)]+)') AS editor_name,
    REGEXP_EXTRACT(JSON_VALUE(log_json, '$.message'), r'editor_id: (\d+)')    AS editor_id,
    CASE
        WHEN REGEXP_EXTRACT(JSON_VALUE(log_json, '$.message'), r'editor_id: (\d+)') IS NOT NULL
        THEN CONCAT(
            CASE region
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-',
            REGEXP_EXTRACT(JSON_VALUE(log_json, '$.message'), r'editor_id: (\d+)'),
            '-',
            TRIM(REPLACE(tenant, '"', ''))
        )
    END AS region_editor_id,
    CASE
        WHEN CAST(JSON_VALUE(log_json, '$.model_id') AS STRING) IS NOT NULL
        THEN CONCAT(
            CASE region
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-',
            CAST(JSON_VALUE(log_json, '$.model_id') AS STRING),
            '-',
            TRIM(REPLACE(tenant, '"', ''))
        )
    END AS region_editing_id,
    log_json,
    TRIM(REPLACE(tenant, '"', '')) AS tenant
  FROM deduped
)

SELECT * FROM final