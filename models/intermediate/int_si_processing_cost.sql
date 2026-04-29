/*
    int_si_processing_costs.sql

    목적: SQA(SiteInsights) editing별 Processing 비용 + 인건비 계산
    소스: int_sqa_tasks + int_editings

    비용 로직:
      - Processing 비용: stat_total_entities × 0.00177055
      - 인건비: (total_duration_sec / 60) / 60 × unit_price / stat_total_entities
        → int_capture_processing_costs와 동일한 방식
        → unit_price: editor_email에 'lts' 포함 시 $5, 아니면 $10.8
*/

WITH sqa_tasks AS (
    SELECT * FROM {{ ref('int_sqa_tasks') }}
),

editings AS (
    SELECT
        region_editing_id,
        stat_total_entities
    FROM {{ ref('int_editings') }}
),

joined AS (
    SELECT
        st.region_editing_id,
        st.region_team_id,
        st.team_name,
        st.region_facility_id,
        st.facility_name,
        st.editor_email,
        st.editing_start_at,
        st.editing_end_at,
        st.review_start_at,
        st.done_at,
        st.total_duration_sec,
        e.stat_total_entities,
        CASE
            WHEN st.editor_email LIKE '%lts%' THEN 5.0
            ELSE 10.8
        END AS editor_unit_price
    FROM sqa_tasks st
    LEFT JOIN editings e ON e.region_editing_id = st.region_editing_id
),

final AS (
    SELECT
        region_editing_id,
        region_team_id,
        team_name,
        region_facility_id,
        facility_name,
        editor_email,
        editor_unit_price,
        editing_start_at,
        done_at,
        stat_total_entities,
        FORMAT_TIMESTAMP('%Y-%m', done_at) AS year_month,

        -- Processing 비용
        COALESCE(stat_total_entities, 0) * 0.00177055 AS processing_cost,

        -- 인건비 (소요시간 분→시간 변환 × 시급, stat_total_entities로 분배)
        SAFE_DIVIDE(
            (COALESCE(total_duration_sec, 0) / 60.0) / 60.0
            * editor_unit_price,
            stat_total_entities
        ) AS editing_labor_cost,

        -- 총 비용
        COALESCE(stat_total_entities, 0) * 0.00177055
        + SAFE_DIVIDE(
            (COALESCE(total_duration_sec, 0) / 60.0) / 60.0
            * editor_unit_price,
            stat_total_entities
        ) AS total_cost

    FROM joined
)

SELECT * FROM final
