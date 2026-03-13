-- stg_seed__team_sf_account_mapping.sql

WITH source AS (
    SELECT * FROM {{ source('seed', 'team_sf_account_mapping') }}
),

renamed AS (
    SELECT
        CAST(_id AS STRING) AS team_id,
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
            CAST(_id AS STRING)
        ) AS region_team_id,
        name AS team_name,
        domain,
        NULLIF(sf_account_id, '') AS sf_account_id
    FROM source
)

SELECT * FROM renamed