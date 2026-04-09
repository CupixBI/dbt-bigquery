WITH shift_hours AS (
    SELECT * FROM {{ ref('int_cqa_shift_hours') }}
)

SELECT
    day_num,
    hour,
    SUM(available_headcount) AS total_available_headcount
FROM shift_hours
GROUP BY 1, 2
ORDER BY 1, 2