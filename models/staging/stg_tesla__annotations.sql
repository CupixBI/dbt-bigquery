WITH source AS (
    SELECT *
    FROM {{ source('tesla', 'annotations') }}
), 

renamed AS (  
    SELECT 
        CAST(_id as STRING) as annotation_id,
        CAST(facility_id as STRING) as facility_id,
        TIMESTAMP(created_at) as created_at,
        CAST(user_id as STRING) as user_id,
        name as annotation_name,
        region,
        cycle_state,
        state
    FROM source
),

final AS (
    SELECT
        annotation_id,
        annotation_name,
        region,  
        facility_id,
        created_at,
        user_id,
        cycle_state,  
        state,  
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
            annotation_id
        ) AS region_annotation_id
    FROM renamed
)

SELECT * FROM final