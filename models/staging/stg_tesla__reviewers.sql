WITH source AS (
    SELECT * FROM {{ source('tesla', 'reviewers') }}
),

renamed AS (
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
        CAST(_id AS STRING) AS reviewer_id,
        CAST(id AS STRING) AS id,
        CAST(user_id AS STRING) AS user_id,
        CAST(reviewable_id AS STRING) AS reviewable_id,
        reviewable_type,
        state,
        comment,
        TIMESTAMP(reviewed_at) AS reviewed_at,
        TIMESTAMP(created_at) AS created_at,
        TIMESTAMP(updated_at) AS updated_at,
        tenant,
    FROM source
),

final AS (
    SELECT
        region,
        region_simplify,
        reviewer_id,
        CONCAT(region_simplify, '-', reviewer_id, '-', tenant) AS region_reviewer_id,
        id,
        user_id,
        CONCAT(region_simplify, '-', user_id, '-', tenant) AS region_user_id,
        reviewable_id,
        CONCAT(region_simplify, '-', reviewable_id, '-', tenant) AS region_reviewable_id,
        reviewable_type,
        state,
        comment,
        reviewed_at,
        created_at,
        updated_at,
        tenant,
    FROM renamed
)

SELECT * FROM final
