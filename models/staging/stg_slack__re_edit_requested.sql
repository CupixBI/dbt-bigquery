WITH source AS(
    SELECT * FROM {{ source("slack", "re_edit_requested_native" )}}
),

renamed AS(
    SELECT
        region,
        record_id,
        capture_id,
        region_record_id,
        region_capture_id,
        TIMESTAMP(created_at) as created_at
    FROM source
)

SELECT * FROM renamed