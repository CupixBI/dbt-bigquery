WITH editors AS (
    SELECT
        editor_name,
        work_part,
        level AS editor_level,
        location
    FROM {{ ref('stg_finance__cqa_editors') }}
),

shift_schedule AS (
    SELECT
        work_part,
        work_day,
        work_started,
        work_finished
    FROM {{ ref('stg_finance__cqa_shift_schedule') }}
    GROUP BY work_part, work_day, work_started, work_finished
),

editor_schedule AS (
    SELECT
        e.editor_name,
        e.work_part,
        e.editor_level,
        e.location,
        s.work_day,
        s.work_started,
        s.work_finished
    FROM editors e
    LEFT JOIN shift_schedule s
        ON e.work_part = s.work_part
),

days AS (
    SELECT
        editor_name,
        work_part,
        editor_level,
        location,
        work_started,
        work_finished,
        day_num
    FROM editor_schedule
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
)

SELECT
    editor_name,
    work_part,
    editor_level,
    location,
    day_num,
    hour
FROM hours
WHERE
    CASE
        WHEN work_finished > work_started
            THEN hour >= work_started AND hour < work_finished
        ELSE
            hour >= work_started OR hour < MOD(work_finished, 24)
    END
