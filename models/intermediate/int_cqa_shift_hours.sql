WITH shift_schedule AS (
    SELECT * FROM {{ ref('stg_finance__cqa_shift_schedule') }}
),

days AS (
    SELECT
        work_part,
        editor_level,
        work_started,
        work_finished,
        headcount,
        day_num
    FROM shift_schedule
    CROSS JOIN UNNEST(
        CASE work_day
            WHEN 'Mon-Fri'  THEN [1,2,3,4,5]
            WHEN 'Thur-Mon' THEN [4,5,6,7,1]
            WHEN 'Wed-Sun'  THEN [3,4,5,6,7]
            WHEN 'Tue-Sat'  THEN [2,3,4,5,6]
        END
    ) AS day_num
),

hours AS (
    SELECT *
    FROM days
    CROSS JOIN UNNEST(GENERATE_ARRAY(0, 23)) AS hour
),

final AS (
    SELECT
        work_part,
        editor_level,
        day_num,
        hour,
        MAX(headcount) AS headcount,
        ANY_VALUE(
            CASE
                WHEN work_finished <= 24
                    THEN hour >= work_started AND hour < work_finished
                ELSE
                    hour >= work_started OR hour < (work_finished - 24)
            END
        ) AS is_working
    FROM hours
    GROUP BY work_part, editor_level, day_num, hour
)

SELECT
    work_part,
    editor_level,
    day_num,
    hour,
    headcount AS available_headcount
FROM final
WHERE is_working
