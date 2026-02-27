WITH source AS (
    SELECT * FROM {{ source('tesla', 'integrations') }}
),

renamed AS (
    SELECT
        region,
        CAST(_id as STRING) as integration_id,
        cycle_state,
        provider as integration_target,
        TIMESTAMP(created_at) as created_at, 
        TIMESTAMP(expired_at) as expired_at, 
        integratable_type as integration_level,
        team_id,
        user_id,
    FROM    
        source
),

final AS(
    SELECT
        region,
        integration_id,
        
        -- [Level ID] Region Prefix
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
            integration_id
        ) AS region_integration_id,
        
        COALESCE(integration_target, 'Unknown') as integration_target,
        COALESCE(integration_level, 'Unknown') as integration_level,

        user_id,

    FROM renamed
)

SELECT * FROM final