/*
    mart_sales_opportunities.sql
    
    목적: Sales 대시보드용 Opportunity 상세 테이블
    
    Grain: Opportunity 1건 = 1행
    
    용도:
      - Closed Won 금액/건수/Unique Customer 집계
      - Stage, Forecast, AE, Region, Org Segment, Contract Type, License Type별 분석
      - Opportunity 상세 테이블
*/

WITH mapped AS (
    SELECT * FROM {{ ref('int_opportunities_enriched') }}
),

final AS (
    SELECT
        -- PK
        opportunity_id,

        -- Opp 기본 정보
        opportunity_name,
        opportunity_type,
        stage_name,
        amount,
        amount_usd,
        currency_code,
        rate_to_usd,
        created_at,
        close_date,
        opp_number,

        -- 계약 정보
        contract_type,
        contract_term_months,
        subscription_start_date,
        subscription_end_date,
        has_site_insights,
        license_capacity_area,
        license_units,

        -- 파이프라인/예측
        probability,
        forecast_category,

        -- 리드
        lead_source,
        lead_type,

        -- Win/Loss
        win_story,
        loss_reason,

        -- Account 정보
        account_id,
        account_name,
        industry,
        vertical,
        market_segment,
        territory,
        website,
        shipping_city,
        shipping_state,
        shipping_country,
        contract_status,
        license_expiration_date,
        first_sw_sale_date,

        -- AE (Opp Owner)
        owner_id,
        owner_name,
        owner_email,
        owner_region,

        -- 대시보드용 파생 컬럼
        CASE
            WHEN stage_name IN ('Closed Won', 'Invoiced') THEN amount_usd
            ELSE 0
        END AS closed_won_amount_usd,

        CASE
            WHEN stage_name IN ('Closed Won', 'Invoiced') THEN 1
            ELSE 0
        END AS is_closed_won,

        -- close_date 기준 연/월 (트렌드 분석용)
        EXTRACT(YEAR FROM close_date) AS close_year,
        EXTRACT(MONTH FROM close_date) AS close_month,
        FORMAT_DATE('%Y-%m', close_date) AS close_year_month

    FROM mapped
)

SELECT * FROM final