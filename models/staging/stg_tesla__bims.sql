WITH source AS (
    SELECT 
        *
    FROM {{ source('tesla', 'bims') }}
),

renamed AS (
    SELECT 
        CAST(_id as STRING) as bim_id,
        name,
        region,
        tenant,
        CAST(facility_id as STRING) as facility_id,
        CAST(last_bim_revision_id AS STRING) as last_bim_revision_id,
        TIMESTAMP(created_at) as created_at,
        cycle_state
    FROM source
),

final AS (
    SELECT
        bim_id,
        name,
        region,
        tenant,
        facility_id,
        last_bim_revision_id,
        created_at,
        cycle_state,
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
            bim_id,
            '-',
            tenant
        ) AS region_bim_id

    FROM renamed
)

SELECT * FROM final