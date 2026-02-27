WITH cqa_editors AS(
    SELECT * FROM {{ source('finance', 'cqa_editors_native') }}
),

cqa_labor_unit_cost AS(
    SELECT * FROM {{ source('finance', 'cqa_labor_unit_cost_native') }}
),

add_location_and_filtered AS(
    SELECT
        editor_name,
        email,
        level,
        work_part,
        CASE work_part
            WHEN 'MF17' THEN 'VN'
            WHEN 'WS06' THEN 'VN'
            ELSE 'KR' 
        END AS location
    FROM cqa_editors
    WHERE state = 'Activate'
),

final AS (
    SELECT
        -- 1. 에디터(editors) 정보는 모두 가져오기
        editors.*,

        -- 2. 단가(costs) 테이블에서 unit_price 가져오기
        CAST(costs.unit_price AS FLOAT64) AS unit_price

    FROM add_location_and_filtered AS editors
    -- [Left Join 수행]
    -- 에디터 정보는 유지하고, 매칭되는 단가 정보가 있을 때만 붙입니다.
    LEFT JOIN cqa_labor_unit_cost AS costs
        ON editors.level = costs.editor_level      -- 레벨 매칭
        AND editors.location = costs.location      -- 지역 매칭 (KR/VN)
)

SELECT * FROM final