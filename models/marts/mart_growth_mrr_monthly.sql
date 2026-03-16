/*
    mart_growth_mrr_monthly.sql
    
    목적: Growth 대시보드용 월별 MRR 분배 테이블
    
    Grain: Opportunity × Month (1 Opp = N개 월 행)
    
    로직:
      - Closed Won + Invoiced Opp만 대상
      - MRR = amount_usd ÷ (start~end 월수)
      - subscription 기간을 월별로 펼침
*/

WITH mapped AS (
    SELECT *
    FROM {{ ref('int_opportunities_enriched') }}
    WHERE stage_name IN ('Closed Won', 'Invoiced')
      AND subscription_start_date IS NOT NULL
      AND subscription_end_date IS NOT NULL
      AND amount_usd IS NOT NULL
      AND amount_usd > 0
),

-- 월수 계산 + MRR 산출
with_mrr AS (
    SELECT
        *,
        DATE_DIFF(subscription_end_date, subscription_start_date, MONTH) AS contract_months,
        SAFE_DIVIDE(
            amount_usd,
            DATE_DIFF(subscription_end_date, subscription_start_date, MONTH)
        ) AS monthly_mrr
    FROM mapped
),

-- 월별 펼치기 (GENERATE_DATE_ARRAY)
month_spine AS (
    SELECT
        opportunity_id,
        month_start
    FROM with_mrr,
    UNNEST(
        GENERATE_DATE_ARRAY(
            DATE_TRUNC(subscription_start_date, MONTH),
            DATE_TRUNC(DATE_SUB(subscription_end_date, INTERVAL 1 DAY), MONTH),
            INTERVAL 1 MONTH
        )
    ) AS month_start
),

final AS (
    SELECT
        m.month_start,
        FORMAT_DATE('%Y-%m', m.month_start) AS year_month,
        EXTRACT(YEAR FROM m.month_start) AS year,
        EXTRACT(MONTH FROM m.month_start) AS month,

        w.opportunity_id,
        w.opportunity_name,
        w.opp_number,
        w.amount,
        w.amount_usd,
        w.currency_code,
        w.contract_months,
        w.monthly_mrr,
        w.contract_type,
        w.subscription_start_date,
        w.subscription_end_date,
        w.has_site_insights,
        w.license_capacity_area,
        w.license_units,

        -- Account
        w.account_id,
        w.account_name,
        w.market_segment,
        w.industry,
        w.territory,

        -- Region
        w.owner_region,

        -- AE
        w.owner_name,
        w.owner_email

    FROM month_spine m
    INNER JOIN with_mrr w
        ON m.opportunity_id = w.opportunity_id
)

SELECT * FROM final