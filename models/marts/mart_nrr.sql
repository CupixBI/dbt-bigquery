/*
    mart_nrr.sql

    목적: Revenue Retention 대시보드용 NRR/GRR 및 MRR 변동 추적

    Grain: Account × Month

    핵심 로직:
      - Account × Month의 last_opp_id (MAX subscription_end_date 기준 Opp)를 기준으로
        구독 사이클 변화 감지
      - last_opp_id가 바뀌는 시점 = 새 구독 사이클 시작
      - 이전 사이클 account 전체 MRR 합산 vs 새 사이클 account 전체 MRR 합산 비교

    변동 유형 분류:
      - New         : 첫 번째 last_opp_id (이전 없음)
      - Renewal     : 새 사이클 MRR이 이전 사이클 MRR ±5% 이내
      - Expansion   : 새 사이클 MRR > 이전 사이클 MRR * 1.05
      - Contraction : 새 사이클 MRR < 이전 사이클 MRR * 0.95
      - Reactivation: 이전 last_opp subscription_end + 3개월 초과 후 새 계약
      - Churn       : last_opp subscription_end + 3개월 후에도 새 계약 없음

    NRR/GRR:
      - 12개월 Rolling 기준
      - NRR = 현재 active_mrr / 12개월 전 active_mrr (기존 고객만)
      - GRR = (현재 active_mrr - expansion_mrr) / 12개월 전 active_mrr
*/

-- ============================================================
-- 1. mart_growth_mrr_monthly → Account × Month 집계
--    + 해당 달의 last_opp_id (MAX subscription_end_date 기준)
-- ============================================================
WITH account_monthly AS (
    SELECT
        month_start,
        FORMAT_DATE('%Y-%m', month_start) AS year_month,
        EXTRACT(YEAR FROM month_start)    AS year,
        EXTRACT(MONTH FROM month_start)   AS month,
        account_id,

        -- 해당 월 account 전체 활성 MRR 합산
        SUM(monthly_mrr)               AS active_mrr,
        COUNT(DISTINCT opportunity_id) AS active_opp_count,

        -- 해당 달의 last_opp_id (MAX subscription_end_date 기준)
        MAX(subscription_end_date)     AS max_subscription_end_date,
        ARRAY_AGG(
            opportunity_id
            ORDER BY subscription_end_date DESC
            LIMIT 1
        )[OFFSET(0)]                   AS last_opp_id

    FROM {{ ref('mart_growth_mrr_monthly') }}
    WHERE account_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),

-- account 메타정보 (최신 월 기준)
account_meta AS (
    SELECT DISTINCT
        account_id,
        account_name,
        market_segment,
        owner_region,
        owner_name,
        owner_email
    FROM {{ ref('mart_growth_mrr_monthly') }}
    WHERE account_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY account_id
        ORDER BY month_start DESC
    ) = 1
),

-- ============================================================
-- 2. last_opp_id 변화 감지
--    이전 달이 아닌 해당 account의 과거 이력 중 가장 최근값 기준
--    (구독 gap이 있어도 정확하게 이전 구독과 비교 가능)
-- ============================================================
account_monthly_with_prev AS (
    SELECT
        *,
        -- 과거 이력 중 가장 최근 last_opp_id (구독 사이클 변화 감지용)
        LAST_VALUE(last_opp_id) OVER (
            PARTITION BY account_id
            ORDER BY month_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_last_opp_id,

        -- 과거 이력 중 가장 최근 active_mrr (구독 사이클 변화 감지용)
        LAST_VALUE(active_mrr) OVER (
            PARTITION BY account_id
            ORDER BY month_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_active_mrr,

        -- 과거 이력 중 가장 최근 max_subscription_end_date (Churn/Renewal gap 판단용)
        LAST_VALUE(max_subscription_end_date) OVER (
            PARTITION BY account_id
            ORDER BY month_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_max_subscription_end_date,

        -- 바로 이전 달 active_mrr (Existing 내 MRR 변화 감지용)
        LAG(active_mrr) OVER (
            PARTITION BY account_id ORDER BY month_start
        ) AS prev_month_active_mrr,

        -- 바로 이전 달 last_opp_id (Existing 여부 판단용)
        LAG(last_opp_id) OVER (
            PARTITION BY account_id ORDER BY month_start
        ) AS prev_month_last_opp_id

    FROM account_monthly
),

-- ============================================================
-- 3. 변동 유형 분류
--    last_opp_id가 바뀌는 시점에만 분류
-- ============================================================
account_monthly_classified AS (
    SELECT
        *,

        -- 변동 유형
        CASE
            -- 첫 구독 (이전 last_opp 없음)
            WHEN prev_last_opp_id IS NULL
                THEN 'New'

            -- last_opp_id 변화 없음 → Existing 내 MRR 변화 체크
            WHEN last_opp_id = prev_month_last_opp_id
                THEN CASE
                    WHEN active_mrr > prev_month_active_mrr * 1.05
                        THEN 'Mid_Expansion'
                    WHEN active_mrr < prev_month_active_mrr * 0.95
                        THEN 'Mid_Contraction'
                    ELSE 'Existing'
                END

            -- 이전 구독 종료 후 3개월 초과 → Reactivation
            WHEN month_start
                > DATE_ADD(prev_max_subscription_end_date, INTERVAL 3 MONTH)
                THEN 'Reactivation'

            -- 3개월 이내 갱신 → MRR 비교
            WHEN active_mrr > prev_active_mrr * 1.05
                THEN 'Expansion'
            WHEN active_mrr < prev_active_mrr * 0.95
                THEN 'Contraction'
            ELSE 'Renewal'
        END AS period_type,

        -- MRR 변동액
        CASE
            WHEN prev_last_opp_id IS NULL
                THEN active_mrr
            WHEN last_opp_id = prev_month_last_opp_id
                AND active_mrr > prev_month_active_mrr * 1.05
                THEN active_mrr - prev_month_active_mrr
            WHEN last_opp_id = prev_month_last_opp_id
                AND active_mrr < prev_month_active_mrr * 0.95
                THEN active_mrr - prev_month_active_mrr
            WHEN last_opp_id = prev_month_last_opp_id
                THEN 0
            WHEN month_start
                > DATE_ADD(prev_max_subscription_end_date, INTERVAL 3 MONTH)
                THEN active_mrr
            ELSE active_mrr - prev_active_mrr
        END AS mrr_change,

        -- Churn 여부: max_subscription_end_date + 3개월 후 next 달에 active_mrr 없음
        CASE
            WHEN LEAD(month_start) OVER (
                PARTITION BY account_id ORDER BY month_start
            ) IS NULL
                AND max_subscription_end_date
                    < DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
                THEN TRUE
            WHEN LEAD(month_start) OVER (
                PARTITION BY account_id ORDER BY month_start
            ) > DATE_ADD(max_subscription_end_date, INTERVAL 3 MONTH)
                THEN TRUE
            ELSE FALSE
        END AS is_churned

    FROM account_monthly_with_prev
),

-- ============================================================
-- 4. 변동 유형별 MRR 집계
-- ============================================================
account_monthly_agg AS (
    SELECT
        am.month_start,
        am.year_month,
        am.year,
        am.month,
        am.account_id,
        m.account_name,
        m.market_segment,
        m.owner_region,
        m.owner_name,
        m.owner_email,
        am.active_mrr,
        am.active_opp_count,

        -- 변동 유형별 MRR
        CASE WHEN am.period_type = 'New'
            THEN am.mrr_change ELSE 0 END          AS new_mrr,
        CASE WHEN am.period_type = 'Renewal'
            THEN am.active_mrr ELSE 0 END           AS renewal_mrr,
        CASE WHEN am.period_type = 'Expansion'
            THEN am.mrr_change ELSE 0 END           AS expansion_mrr,
        CASE WHEN am.period_type = 'Contraction'
            THEN ABS(am.mrr_change) ELSE 0 END      AS contraction_mrr,
        CASE WHEN am.period_type = 'Reactivation'
            THEN am.mrr_change ELSE 0 END           AS reactivation_mrr,

        -- Existing 내 MRR 변화 (mid)
        CASE WHEN am.period_type = 'Mid_Expansion'
            THEN am.mrr_change ELSE 0 END           AS mid_expansion_mrr,
        CASE WHEN am.period_type = 'Mid_Contraction'
            THEN ABS(am.mrr_change) ELSE 0 END      AS mid_contraction_mrr,

        -- Churn MRR: subscription_end + 3개월 달에 기록
        CASE
            WHEN am.is_churned = TRUE
             AND DATE_TRUNC(
                DATE_ADD(am.max_subscription_end_date, INTERVAL 3 MONTH),
                MONTH
             ) = am.month_start
            THEN am.active_mrr ELSE 0
        END AS churn_mrr,

        CASE
            WHEN am.is_churned = TRUE
             AND DATE_TRUNC(
                DATE_ADD(am.max_subscription_end_date, INTERVAL 3 MONTH),
                MONTH
             ) = am.month_start
            THEN 1 ELSE 0
        END AS churned_account_count,

        am.period_type,
        am.last_opp_id

    FROM account_monthly_classified am
    LEFT JOIN account_meta m
        ON am.account_id = m.account_id
)

SELECT
    month_start,
    year_month,
    year,
    month,
    account_id,
    account_name,
    market_segment,
    owner_region,
    owner_name,
    owner_email,
    active_mrr,
    active_opp_count,
    period_type,
    last_opp_id,
    new_mrr,
    renewal_mrr,
    expansion_mrr,
    contraction_mrr,
    reactivation_mrr,
    mid_expansion_mrr,
    mid_contraction_mrr,
    churn_mrr,
    churned_account_count,
    CURRENT_TIMESTAMP() AS updated_at

FROM account_monthly_agg