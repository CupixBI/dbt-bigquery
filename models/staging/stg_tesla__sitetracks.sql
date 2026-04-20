WITH source AS (
    SELECT * FROM {{ source('tesla', 'sitetracks') }}
),

renamed AS (
    SELECT
        CASE region
            WHEN 'uswe2' THEN 'US'
            WHEN 'apse2' THEN 'AU'
            WHEN 'euce1' THEN 'EU'
            WHEN 'apne1' THEN 'JP'
            WHEN 'apse1' THEN 'SG'
            WHEN 'cace1' THEN 'CA'
            ELSE 'Unknown'
        END AS region,
        TIMESTAMP(created_at) as created_at,
        CAST(_id AS STRING) AS sitetrack_id,
        processing_status,
        CAST(team_id AS STRING) AS team_id,
        TIMESTAMP(state_updated_at) as state_updated_at,
        CAST(target_id AS STRING) AS record_id,
        target_type,
        CAST(level_id AS STRING) AS level_id,
        CAST(facility_id AS STRING) AS facility_id,
        sys.stat_size_total_elements,
        sys.sitetrack_version,
        TIMESTAMP(sys.sitetrack_finished_at) AS sitetrack_finished_at,
        TIMESTAMP(sys.sitetrack_started_at) AS sitetrack_started_at,
        sys.revision,
        TIMESTAMP(done_at) as done_at,
        error_code,
        progress,
        tenant
    FROM source
),

final AS (
    SELECT
        region,
        sitetrack_id,
        tenant,
        CONCAT(region, '-', sitetrack_id, '-', tenant) AS region_sitetrack_id,
        processing_status,
        team_id,
        state_updated_at,
        record_id,
        CONCAT(region, '-', record_id, '-', tenant) AS region_record_id,
        target_type,
        level_id,
        facility_id,
        stat_size_total_elements,
        sitetrack_version,
        sitetrack_finished_at,
        sitetrack_started_at,
        revision,
        done_at,
        error_code,
        progress,
    FROM renamed
)

SELECT * FROM final
