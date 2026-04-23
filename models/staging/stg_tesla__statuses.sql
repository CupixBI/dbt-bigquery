WITH source AS (
    SELECT * FROM {{ source('tesla', 'statuses') }}
),

renamed AS (
    SELECT
        region,
        CAST(_id AS STRING) AS status_id,
        CAST(id AS STRING) AS id,
        tenant,
        name AS status_name,
        kind,
        value,
        status_type_code,
        CAST(status_type_id AS STRING) AS status_type_id,
        is_complete_status,
        cycle_state,
        cycle_state_updated_at,
        CAST(cycle_state_updated_by_id AS STRING) AS cycle_state_updated_by_id,
        CAST(team_id AS STRING) AS team_id,
        CAST(user_id AS STRING) AS user_id,
        CAST(workflow_id AS STRING) AS workflow_id,
        row_order,
        progress,
        cached,
        updated_at,
        created_at,
    FROM source
),

final AS (
    SELECT
        region,
        status_id,
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
            status_id,
            '-',
            tenant
        ) AS region_status_id,

        id,
        tenant,
        status_name,
        kind,
        value,
        status_type_code,
        status_type_id,
        is_complete_status,
        cycle_state,
        cycle_state_updated_at,
        cycle_state_updated_by_id,

        team_id,
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
            team_id,
            '-',
            tenant
        ) AS region_team_id,

        user_id,
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
            user_id,
            '-',
            tenant
        ) AS region_user_id,

        workflow_id,
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
            workflow_id,
            '-',
            tenant
        ) AS region_workflow_id,

        row_order,
        progress,
        cached,
        updated_at,
        created_at,
    FROM renamed
)

SELECT * FROM final
