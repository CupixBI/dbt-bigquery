WITH sitetracks AS (
    SELECT * FROM {{ ref('stg_tesla__sitetracks') }}
),

final AS (
    SELECT
        region,
        sitetrack_id,
        region_sitetrack_id,
        processing_status,
        team_id,
        state_updated_at,
        TIMESTAMP_ADD(state_updated_at, INTERVAL 9 HOUR) AS state_updated_at_kst,
        record_id,
        target_type,
        error_code,
        level_id,
        facility_id,
        stat_size_total_elements,
        sitetrack_version,
        sitetrack_finished_at,
        TIMESTAMP_ADD(sitetrack_finished_at, INTERVAL 9 HOUR) AS sitetrack_finished_at_kst,
        sitetrack_started_at,
        TIMESTAMP_ADD(sitetrack_started_at, INTERVAL 9 HOUR) AS sitetrack_started_at_kst,
        revision,
        done_at,
        TIMESTAMP_ADD(done_at, INTERVAL 9 HOUR) AS done_at_kst,
        progress
    FROM sitetracks
)

SELECT * FROM final