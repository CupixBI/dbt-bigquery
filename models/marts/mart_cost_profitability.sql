/*
    mart_cost_profitability.sql
    
    목적: Cost & Profitability 대시보드용
    
    Grain: Account × Month
    
    로직:
      - MRR: mart_growth_mrr_monthly에서 account별 월별 집계 (USD)
      - Cost: int_capture_processing_costs에서 team별 월별 집계 후
              account ↔ team 매핑으로 account에 합산
      - Gross Profit = MRR - Total Cost
*/

WITH account_team_mapping AS (
    -- sf_resource_id로 매핑
    SELECT DISTINCT
        sf_resource_id AS account_id,
        region_team_id
    FROM {{ ref('stg_tesla__teams') }}
    WHERE sf_resource_id != 'Unknown'
    
    UNION DISTINCT
    
    -- seed로 매핑
    SELECT DISTINCT
        sf_account_id AS account_id,
        region_team_id
    FROM {{ ref('stg_seed__team_sf_account_mapping') }}
    WHERE sf_account_id IS NOT NULL
),

monthly_mrr AS (
    SELECT
        account_id,
        year_month,
        month_start,
        SUM(monthly_mrr) AS total_mrr,
        COUNT(DISTINCT opportunity_id) AS active_subscriptions
    FROM {{ ref('mart_growth_mrr_monthly') }}
    GROUP BY 1, 2, 3
),

team_monthly_cost AS (
    SELECT
        region_team_id,
        year_month,
        SUM(processing_cost) AS total_processing_cost,
        SUM(editing_labor_cost) AS total_editing_cost,
        SUM(total_capture_cost) AS total_cost,
        COUNT(*) AS capture_count
    FROM {{ ref('int_capture_processing_costs') }}
    WHERE region_team_id IS NOT NULL
    GROUP BY 1, 2
),

account_monthly_cost AS (
    SELECT
        atm.account_id,
        tc.year_month,
        SUM(tc.total_processing_cost) AS total_processing_cost,
        SUM(tc.total_editing_cost) AS total_editing_cost,
        SUM(tc.total_cost) AS total_cost,
        SUM(tc.capture_count) AS capture_count
    FROM team_monthly_cost tc
    INNER JOIN account_team_mapping atm
        ON tc.region_team_id = atm.region_team_id
    GROUP BY 1, 2
),

accounts AS (
    SELECT
        account_id,
        account_name,
        owner_id
    FROM {{ ref('stg_salesforce__accounts') }}
),

sf_users AS (
    SELECT
        sf_user_id,
        full_name,
        email,
        region
    FROM {{ ref('stg_salesforce__users') }}
),

final AS (
    SELECT
        COALESCE(m.account_id, c.account_id) AS account_id,
        a.account_name,
        u.full_name AS account_owner_name,
        u.email AS account_owner_email,
        u.region AS account_owner_region,
        COALESCE(m.year_month, c.year_month) AS year_month,
        m.month_start,

        -- MRR (USD)
        COALESCE(m.total_mrr, 0) AS total_mrr,
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
    FULL OUTER JOIN account_monthly_cost c
        ON m.account_id = c.account_id
        AND m.year_month = c.year_month
    LEFT JOIN accounts a
        ON COALESCE(m.account_id, c.account_id) = a.account_id
    LEFT JOIN sf_users u
        ON a.owner_id = u.sf_user_id
)

SELECT * FROM final