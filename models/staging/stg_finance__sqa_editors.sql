WITH source AS (
    SELECT * FROM {{ source('finance', 'sqa_labor_unit_cost_native') }}
),

renamed AS (
    SELECT
        editor_level,
        location,
        unit_cost AS unit_price,
        unit
    FROM source
)

SELECT * FROM renamed