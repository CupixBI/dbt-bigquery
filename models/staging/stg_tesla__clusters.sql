WITH source AS (
    SELECT * FROM {{ source('tesla', 'clusters') }}
),

renamed AS (
    SELECT
        region,
        CAST(_id AS STRING) AS cluster_id,
        CAST(capture_id AS STRING) AS capture_id,
        kind,
        skat_result_type,
        meta.preview.align_preview_meta_refinement_with_prior_map IS NOT NULL AS has_refinement_result,
        tenant
    FROM source
),

final AS (
    SELECT
        region,
        cluster_id,
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
            cluster_id,
            '-',
            tenant
        ) AS region_cluster_id,
        capture_id,
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
            capture_id,
            '-',
            tenant
        ) AS region_capture_id,
        kind,
        skat_result_type,
        has_refinement_result,
        tenant
    FROM renamed
)

SELECT * FROM final
