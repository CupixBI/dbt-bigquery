WITH shift_hours AS (
    SELECT * FROM {{ ref('int_cqa_shift_hours') }}
)

SELECT
    editor_name,
    work_part,
    editor_level,
    location,
    day_num,
    hour
FROM shift_hours
ORDER BY work_part, day_num, hour, editor_name