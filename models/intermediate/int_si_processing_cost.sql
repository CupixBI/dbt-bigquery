/*
    int_si_processing_costs.sql
    
    목적: Element 개수 당 Processing 비용 + Editing 인건비 계산
    
    비용 로직:
      - Processing 비용: total_element × 0.00166481
      - Editing 인건비: (edit_min + review_min) / 60 × editor unit_price(시급)
        → 같은 job(region_team_id + edit_started_at) 내 실제 Status 변경된 Element 개수
*/

{% set processing_unit_price_per_element = 0.00166481 %}

WITH editing AS (
    SELECT * FROM {{ ref('stg_tesla__editings') }}
    WHERE editing_type = 'siteinsights'
),

labor_cost AS (
    SELECT * FROM {{ ref('stg_finance__sqa_editors') }}
),

-- job 내 실제 Status 변경한 element 수 합산 (인건비 분배용)
with_job_element_count AS (
    SELECT
        *,
        -- TODO: 실제 status 변경 element 수 데이터 확보 후 교체 필요
        -- 현재는 stat_total_entities로 대체
        CASE
            WHEN region_team_id IS NOT NULL AND edit_started_at IS NOT NULL
            THEN SUM(stat_total_entities) OVER (
                PARTITION BY region_team_id, edit_started_at
            )
            ELSE stat_total_entities
        END AS job_total_entities
    FROM editing
),

final AS (
    SELECT
        e.editing_id,
        e.created_at,
        e.created_at_kst,
        e.state_updated_at,
        e.state_updated_at_kst,
        e.editor_id,
        e.editor_email,
        e.editor_name,
        e.editor_level,
        e.stat_total_entities,
        lc.unit_price AS editor_unit_price,

        -- Processing 비용
        {{ processing_unit_price_per_element }} * e.stat_total_entities AS processing_cost,

        -- Editing 인건비
        SAFE_DIVIDE(
            (
                COALESCE(e.edit_min, 0)
                + COALESCE(e.review_min, 0)
            ) / 60 * COALESCE(lc.unit_price, 12.31),
            e.job_total_entities
        ) AS editing_labor_cost

    FROM with_job_element_count e
    LEFT JOIN labor_cost lc
        ON e.editor_level = lc.editor_level
        AND e.location = lc.location  -- location 컬럼 Editing에 있는지 확인 필요!
)

SELECT * FROM final