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

with_json_region AS (
  SELECT
    *,
    CASE REGEXP_EXTRACT(JSON_VALUE(log_json, '$.ddtags'), r'region:([^,]+)')
        WHEN 'us-west-2'      THEN 'uswe2'
        WHEN 'ap-southeast-2' THEN 'apse2'
        WHEN 'eu-central-1'   THEN 'euce1'
        WHEN 'ap-northeast-1' THEN 'apne1'
        WHEN 'ap-southeast-1' THEN 'apse1'
        WHEN 'ca-central-1'   THEN 'cace1'
    END AS json_region
  FROM deduped
),

final AS (
  SELECT
    capture_trace_id,
    region,
    json_region,
    CONCAT(
        CASE json_region
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
    JSON_VALUE(log_json, '$.remark')     AS stage,
    JSON_VALUE(log_json, '$.class_name') AS class_name,
    JSON_VALUE(log_json, '$.class')      AS class,
    CASE
        WHEN JSON_VALUE(log_json, '$.class_name') = 'Editing' OR JSON_VALUE(log_json, '$.class') = 'Editing'
        THEN CAST(JSON_VALUE(log_json, '$.model_id') AS STRING)
        ELSE NULL
    END AS editing_id,
    CASE
        WHEN JSON_VALUE(log_json, '$.class_name') = 'Capture' OR JSON_VALUE(log_json, '$.class') = 'Capture'
        THEN CAST(JSON_VALUE(log_json, '$.model_id') AS STRING)
        ELSE NULL
    END AS capture_id,
    REGEXP_EXTRACT(JSON_VALUE(log_json, '$.message'), r'editor_name: ([^)]+)') AS editor_name,
    REGEXP_EXTRACT(JSON_VALUE(log_json, '$.message'), r'editor_id: (\d+)')     AS editor_id,
    CASE
        WHEN REGEXP_EXTRACT(JSON_VALUE(log_json, '$.message'), r'editor_id: (\d+)') IS NOT NULL
        THEN CONCAT(
            CASE json_region
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
        WHEN (JSON_VALUE(log_json, '$.class_name') = 'Editing' OR JSON_VALUE(log_json, '$.class') = 'Editing')
            AND CAST(JSON_VALUE(log_json, '$.model_id') AS STRING) IS NOT NULL
        THEN CONCAT(
            CASE json_region
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
    CASE
        WHEN (JSON_VALUE(log_json, '$.class_name') = 'Capture' OR JSON_VALUE(log_json, '$.class') = 'Capture')
            AND CAST(JSON_VALUE(log_json, '$.model_id') AS STRING) IS NOT NULL
        THEN CONCAT(
            CASE json_region
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
    END AS region_capture_id,
    log_json,
    TRIM(REPLACE(tenant, '"', '')) AS tenant
  FROM with_json_region
)

SELECT * FROM final
