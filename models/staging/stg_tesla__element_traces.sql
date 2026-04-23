WITH source AS (
    SELECT * FROM {{ source('tesla', 'element_traces') }}
),

renamed AS (
    SELECT
        region,
        CAST(_id AS STRING) AS element_trace_id,
        CAST(id AS STRING) AS id,
        tenant,
        CAST(bim_revision_id AS STRING) AS bim_revision_id,
        bim_external_id,
        cycle_state,
        cycle_state_updated_at,
        CAST(cycle_state_updated_by_id AS STRING) AS cycle_state_updated_by_id,
        CAST(deviation_id AS STRING) AS deviation_id,
        CAST(element_id AS STRING) AS element_id,
        CAST(facility_id AS STRING) AS facility_id,
        CAST(team_id AS STRING) AS team_id,
        CAST(workspace_id AS STRING) AS workspace_id,
        CAST(user_id AS STRING) AS user_id,
        CAST(record_id AS STRING) AS record_id,
        CAST(sitetrack_id AS STRING) AS sitetrack_id,
        CAST(category_id AS STRING) AS category_id,
        CAST(task_id AS STRING) AS task_id,
        CAST(phase_id AS STRING) AS phase_id,
        CAST(estimated_status_id AS STRING) AS estimated_status_id,
        CAST(workarea_id AS STRING) AS workarea_id,
        CAST(status_id AS STRING) AS status_id,
        CAST(vendor_id AS STRING) AS vendor_id,
        CAST(texture_id AS STRING) AS texture_id,
        purpose,
        activity_key,
        processing_result.points_validator.coverage AS processing_result_coverage,
        processing_result.points_validator.trust_level AS processing_result_trust_level,
        processing_result.sitetrack_version AS processing_sitetrack_version,
        updated_at,
        created_at,
    FROM source
),

final AS (
    SELECT
        region,
        element_trace_id,
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
            element_trace_id,
            '-',
            tenant
        ) AS region_element_trace_id,

        id,
        tenant,
        bim_revision_id,
        bim_external_id,
        cycle_state,
        cycle_state_updated_at,
        cycle_state_updated_by_id,

        deviation_id,
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
            deviation_id,
            '-',
            tenant
        ) AS region_deviation_id,

        element_id,
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
            element_id,
            '-',
            tenant
        ) AS region_element_id,

        facility_id,
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
            facility_id,
            '-',
            tenant
        ) AS region_facility_id,

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

        workspace_id,
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
            workspace_id,
            '-',
            tenant
        ) AS region_workspace_id,

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

        record_id,
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
            record_id,
            '-',
            tenant
        ) AS region_record_id,

        sitetrack_id,
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
            sitetrack_id,
            '-',
            tenant
        ) AS region_sitetrack_id,

        category_id,
        task_id,
        phase_id,
        estimated_status_id,
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
            estimated_status_id,
            '-',
            tenant
        ) AS region_estimated_status_id,
        workarea_id,
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
        vendor_id,
        texture_id,
        purpose,
        activity_key,
        processing_result_coverage,
        processing_result_trust_level,
        processing_sitetrack_version,
        updated_at,
        created_at,
    FROM renamed
)

SELECT * FROM final
