/*
    mart_customer_value_maturation.sql
    
    목적: Customer Value Maturation 대시보드용
    
    Grain: Account × Year
    
    로직:
      - Closed Won + Invoiced Opp만 대상
      - close_date 기준 연도별 Account 금액 집계 (USD)
      - 올해/작년은 YTD(현재 월 기준) 컬럼 추가
      - Tier는 total_spending_usd 기준 분류
*/

WITH current_info AS (
    SELECT
        EXTRACT(YEAR FROM CURRENT_DATE()) AS current_year,
        EXTRACT(MONTH FROM CURRENT_DATE()) AS current_month
),

won_opps AS (
    SELECT *
    FROM {{ ref('int_opportunities_enriched') }}
    WHERE stage_name IN ('Closed Won', 'Invoiced')
      AND close_date IS NOT NULL
      AND amount_usd IS NOT NULL
      AND amount_usd > 0
),

account_year_agg AS (
    SELECT
        o.account_id,
        o.account_name,
        o.market_segment,
        o.owner_region,
        o.owner_name,
        EXTRACT(YEAR FROM o.close_date) AS close_year,

        -- 연도 전체 합계 (USD)
        SUM(o.amount_usd) AS total_spending_usd,
        COUNT(*) AS deal_count,
        AVG(o.amount_usd) AS avg_deal_size_usd,
        AVG(o.contract_term_months) AS avg_contract_duration_months,
        SAFE_DIVIDE(
            SUM(o.amount_usd * o.contract_term_months),
            SUM(o.amount_usd)
        ) AS wavg_contract_duration_months,

        -- YTD 합계 (올해/작년만)
        SUM(
            CASE
                WHEN EXTRACT(YEAR FROM o.close_date) >= c.current_year - 1
                 AND EXTRACT(MONTH FROM o.close_date) <= c.current_month
                THEN o.amount_usd
            END
        ) AS ytd_spending_usd,

        SUM(
            CASE
                WHEN EXTRACT(YEAR FROM o.close_date) >= c.current_year - 1
                 AND EXTRACT(MONTH FROM o.close_date) <= c.current_month
                THEN 1
            END
        ) AS ytd_deal_count

    FROM won_opps o
    CROSS JOIN current_info c
    GROUP BY 1, 2, 3, 4, 5, 6, c.current_year, c.current_month
),

with_tier AS (
    SELECT
        a.*,
        c.current_year,

        -- Tier (연도 전체 기준)
        CASE
            WHEN total_spending_usd >= 1000000 THEN 'A. >$1M'
            WHEN total_spending_usd >= 500000 THEN 'B. $500K'
            WHEN total_spending_usd >= 250000 THEN 'C. $250K'
            WHEN total_spending_usd >= 100000 THEN 'D. $100K'
            WHEN total_spending_usd >= 50000 THEN 'E. $50K'
            WHEN total_spending_usd >= 10000 THEN 'F. $10K'
            ELSE 'G. <$10K'
        END AS tier,

        -- YTD Tier (올해/작년만)
        CASE
            WHEN ytd_spending_usd >= 1000000 THEN 'A. <$1M'
            WHEN ytd_spending_usd >= 500000 THEN 'B. $500K'
            WHEN ytd_spending_usd >= 250000 THEN 'C. $250K'
            WHEN ytd_spending_usd >= 100000 THEN 'D. $100K'
            WHEN ytd_spending_usd >= 50000 THEN 'E. $50K'
            WHEN ytd_spending_usd >= 10000 THEN 'F. $10K'
            WHEN ytd_spending_usd IS NOT NULL THEN 'G. <$10K'
        END AS ytd_tier,

        CASE WHEN total_spending_usd > 10000 THEN TRUE ELSE FALSE END AS is_over_10k

    FROM account_year_agg a
    CROSS JOIN current_info c
),

final AS (
    SELECT
        *,
        DATE(close_year, 1, 1) AS close_year_date,
        -- YoY (YTD 기준, 올해 vs 작년만)
        CASE
            WHEN close_year = current_year THEN
                LAG(ytd_spending_usd) OVER (
                    PARTITION BY account_id ORDER BY close_year
                )
        END AS prev_year_ytd_spending_usd,

        CASE
            WHEN close_year = current_year THEN
                SAFE_DIVIDE(
                    ytd_spending_usd - LAG(ytd_spending_usd) OVER (
                        PARTITION BY account_id ORDER BY close_year
                    ),
                    LAG(ytd_spending_usd) OVER (
                        PARTITION BY account_id ORDER BY close_year
                    )
                )
        END AS ytd_yoy_pct

    FROM with_tier
)

SELECT * FROM final