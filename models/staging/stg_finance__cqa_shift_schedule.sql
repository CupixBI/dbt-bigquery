

WITH source AS (
    SELECT * FROM {{ source('finance', 'cqa_shift_schedule_native') }}
),

renamed AS (
    SELECT
        work_part,
        work_day,
        CAST(work_started AS INT64) AS work_started,
        CAST(work_finished AS INT64) AS work_finished,
        CAST(break_started AS FLOAT64) AS break_started,
        CAST(break_finished AS FLOAT64) AS break_finished,
        CAST(headcount AS INT64) AS headcount,
        CAST(breakheadcount AS INT64) AS break_headcount
    FROM source
)

SELECT * FROM renamed