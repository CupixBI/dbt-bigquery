WITH shift_schedule AS (
    SELECT * FROM {{ ref('stg_finance__cqa_shift_schedule') }}
),

editors AS (
    SELECT * FROM {{ ref('stg_finance__cqa_editors') }}
),

headcount AS (
    SELECT
        work_part,
        COUNT(*) AS headcount
    FROM editors
    GROUP BY work_part
),

days AS (
    SELECT
        s.work_part,
        s.work_day,
        s.work_started,
        s.work_finished,
        s.break_started,
        s.break_finished,
        s.break_headcount,
        COALESCE(h.headcount, 0) AS headcount,
        day_num
    FROM shift_schedule s
    LEFT JOIN headcount h USING (work_part)
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
        day_num,
        hour,
        MAX(headcount) AS headcount,
        -- 근무 시간 여부 (행마다 동일하므로 ANY_VALUE 사용)
        ANY_VALUE(
            CASE
                WHEN work_finished <= 24
                    THEN hour >= work_started AND hour < work_finished
                ELSE
                    hour >= work_started OR hour < (work_finished - 24)
            END
        ) AS is_working,
        -- 브레이크 여러 행 중 하나라도 해당되면 true
        LOGICAL_OR(
            CASE
                WHEN break_finished <= 24
                    THEN hour >= break_started AND hour < break_finished
                ELSE
                    hour >= break_started OR hour < (break_finished - 24)
            END
        ) AS is_on_break,
        -- 브레이크 중 남은 인원 (가장 적은 값)
        MIN(break_headcount) AS break_headcount
    FROM hours
    GROUP BY work_part, day_num, hour
)

SELECT
    work_part,
    day_num,
    hour,
    CASE
        WHEN NOT is_working THEN 0
        WHEN is_on_break THEN break_headcount
        ELSE headcount
    END AS available_headcount
FROM final
WHERE is_working