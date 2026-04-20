WITH source AS (
    SELECT * FROM {{ source('tesla', 'floorplans') }}
),

renamed AS (
    SELECT
        region,
        CAST(_id AS STRING) AS floorplan_id,
        CAST(level_id AS STRING) AS level_id,
        floorplan_type,
        tenant
    FROM source
),

final AS (
    SELECT
        region,
        floorplan_id,
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
            floorplan_id,
            '-',
            tenant
        ) AS region_floorplan_id,
        level_id,
        floorplan_type,
        tenant
    FROM renamed
)

SELECT * FROM final
