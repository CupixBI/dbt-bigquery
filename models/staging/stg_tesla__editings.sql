WITH source AS (
    SELECT * FROM {{ source('tesla', 'editings') }}
),

renamed AS(
    SELECT
        region,
        CASE region
            WHEN 'uswe2' THEN 'US'
            WHEN 'apse2' THEN 'AU'
            WHEN 'euce1' THEN 'EU'
            WHEN 'apne1' THEN 'JP'
            WHEN 'apse1' THEN 'SG'
            WHEN 'cace1' THEN 'CA'
            ELSE 'Unknown'
        END AS region_simplify,
        CAST(_id as STRING) as editing_id,
        stat_total_entities,
        TIMESTAMP(created_at) as created_at,
        TIMESTAMP(estimated_finish_at) as estimated_finish_at,
        TIMESTAMP(state_updated_at) as state_updated_at,
        TIMESTAMP(updated_at) as updated_at,
        TIMESTAMP(assigned_at) as assigned_at,
        state,
        CAST(editor_id as STRING) as editor_id,
        editing_type,
        CAST(cupix_trace_id as STRING) as capture_trace_id,
        CAST(level_id as STRING) as level_id,
        CAST(record_id as STRING) as record_id,
        CAST(parent_id as STRING) as parent_id,
        tenant,
        CAST(facility_id as STRING) as facility_id,
        sys.preview_quality AS preview_quality,
        CAST(team_id as STRING) as team_id,


    FROM source
),

final AS(
    SELECT
        region,
        region_simplify,
        editing_id,
    
        CONCAT(
            region_simplify,
            '-',
            editing_id,
            '-',
            tenant
        ) AS region_editing_id,
        
        stat_total_entities,
        created_at,
        estimated_finish_at,
        state_updated_at,
        updated_at,
        assigned_at,
        state,
        editor_id,
        editing_type,
        capture_trace_id,
                level_id,
        record_id,
        parent_id,
        tenant,
        facility_id,
        preview_quality,
        team_id,

    FROM renamed
)

SELECT * FROM final