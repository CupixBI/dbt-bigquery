/*
    int_si_processing_costs.sql

    목적: Element Trace 월별 Processing 비용 계산
    소스: stg_tesla__element_traces

    비용 로직:
      - Processing 비용: total_element_count × 0.00177055
      - SQA 인건비: (edit_min + review_min) / 60 × unit_price — TODO
*/

{% set processing_unit_price_per_element = 0.00177055 %}

WITH element_traces AS (
    SELECT * FROM {{ ref('stg_tesla__element_traces') }}
),

facilities AS (
    SELECT
        region_facility_id,
        tenant,
        facility_name,
        workspace_name,
        team_name
    FROM {{ ref('int_facility_details') }}
)

SELECT
    FORMAT_DATE('%Y-%m', DATE(et.created_at, 'Asia/Seoul'))  AS year_month,
    et.region,
    et.tenant,
    et.region_team_id,
    f.team_name,
    f.workspace_name,
    f.facility_name,
    COALESCE(CAST(et.processing_result_trust_level AS STRING), 'Not Evaluated') AS trust_level,
    COUNT(*)                                                  AS total_element_count,
    COUNT(*) * {{ processing_unit_price_per_element }}        AS processing_cost

    -- TODO: sqa_labor_cost (edit_min + review_min 기반 인건비)

FROM element_traces et
LEFT JOIN facilities f
    ON et.region_facility_id = f.region_facility_id

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
