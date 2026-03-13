/*
    mart_cost_profitability.sql
    
    목적: Cost & Profitability 대시보드용
    
    Grain: Team(region_team_id) × Month
    
    로직:
      - MRR: mart_growth_mrr_monthly에서 팀별 월별 집계 (USD)
      - Cost: int_capture_processing_costs에서 팀별 월별 집계
      - Gross Profit = MRR - Total Cost
      - 추후 COGS 유형 추가 시 컬럼 확장
*/

WITH monthly_mrr AS (
    SELECT
        region_team_id,
        team_name,
        region,
        year_month,
        month_start,
        SUM(monthly_mrr) AS total_mrr,
        COUNT(DISTINCT account_id) AS active_accounts,
        COUNT(DISTINCT opportunity_id) AS active_subscriptions
    FROM {{ ref('mart_growth_mrr_monthly') }}
    GROUP BY 1, 2, 3, 4, 5
),

monthly_cost AS (
    SELECT
        region_team_id,
        year_month,
        DATE_TRUNC(DATE(uploading_finished_at), MONTH) AS month_start,
        SUM(processing_cost) AS total_processing_cost,
        SUM(editing_labor_cost) AS total_editing_cost,
        SUM(total_capture_cost) AS total_cost,
        COUNT(*) AS capture_count
    FROM {{ ref('int_capture_processing_costs') }}
    WHERE region_team_id IS NOT NULL
    GROUP BY 1, 2, 3
),

final AS (
    SELECT
        COALESCE(m.region_team_id, c.region_team_id) AS region_team_id,
        COALESCE(m.team_name, t.team_name) AS team_name,
        COALESCE(m.region, t.region) AS region,
        COALESCE(m.year_month, c.year_month) AS year_month,
        COALESCE(m.month_start, c.month_start) AS month_start,

        -- MRR (USD)
        COALESCE(m.total_mrr, 0) AS total_mrr,
        COALESCE(m.active_accounts, 0) AS active_accounts,
        COALESCE(m.active_subscriptions, 0) AS active_subscriptions,

        -- Cost (유형별)
        COALESCE(c.total_processing_cost, 0) AS processing_cost,
        COALESCE(c.total_editing_cost, 0) AS editing_labor_cost,
        COALESCE(c.total_cost, 0) AS total_cost,
        COALESCE(c.capture_count, 0) AS capture_count,

        -- Profitability
        COALESCE(m.total_mrr, 0) - COALESCE(c.total_cost, 0) AS gross_profit,
        SAFE_DIVIDE(
            COALESCE(m.total_mrr, 0) - COALESCE(c.total_cost, 0),
            COALESCE(m.total_mrr, 0)
        ) AS gross_margin_pct

    FROM monthly_mrr m
    FULL OUTER JOIN monthly_cost c
        ON m.region_team_id = c.region_team_id
        AND m.year_month = c.year_month
    LEFT JOIN {{ ref('stg_tesla__teams') }} t
        ON c.region_team_id = t.region_team_id
)

SELECT * FROM final