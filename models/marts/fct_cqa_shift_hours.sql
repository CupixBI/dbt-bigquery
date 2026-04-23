WITH shift_hours AS (
    SELECT * FROM {{ ref('int_cqa_shift_hours') }}
)

SELECT
    work_part,
    editor_level,
    day_num,
    hour,
    SUM(available_headcount) AS total_available_headcount
FROM shift_hours
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4