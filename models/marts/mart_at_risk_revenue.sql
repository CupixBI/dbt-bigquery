/*
    mart_at_risk_revenue.sql
    
    목적: At-Risk Revenue 대시보드용
    
    Grain: Opportunity 1건 = 1행
    
    At-Risk 분류 (CURRENT_DATE 기준):
      - Expiring: subscription_end까지 3개월 미만
      - Early At-Risk: subscription_end 이후 0~2개월
      - Late At-Risk: subscription_end 이후 3~6개월
*/

WITH enriched AS (
    SELECT *
    FROM {{ ref('int_opportunities_enriched') }}
    WHERE stage_name IN ('Closed Won', 'Invoiced')
      AND subscription_end_date IS NOT NULL
      AND amount_usd IS NOT NULL
      AND amount_usd > 0
),

accounts AS (
    SELECT
        account_id,
        csm_id
    FROM {{ ref('stg_salesforce__accounts') }}
),

sf_users AS (
    SELECT
        sf_user_id,
        full_name,
        email
    FROM {{ ref('stg_salesforce__users') }}
),

with_risk AS (
    SELECT
        e.*,
        a.csm_id,

        -- subscription_end까지 남은 개월 수
        DATE_DIFF(e.subscription_end_date, CURRENT_DATE(), MONTH) AS months_to_end,

        -- subscription_end 이후 경과 개월 수
        DATE_DIFF(CURRENT_DATE(), e.subscription_end_date, MONTH) AS months_since_end,

        -- MRR 계산
        SAFE_DIVIDE(
            e.amount_usd,
            DATE_DIFF(e.subscription_end_date, e.subscription_start_date, MONTH)
        ) AS opp_mrr,

        -- At-Risk 분류
        CASE
            WHEN e.subscription_end_date > CURRENT_DATE()
                 AND DATE_DIFF(e.subscription_end_date, CURRENT_DATE(), MONTH) < 3
            THEN 'Expiring'
            WHEN e.subscription_end_date <= CURRENT_DATE()
                 AND DATE_DIFF(CURRENT_DATE(), e.subscription_end_date, MONTH) <= 2
            THEN 'Early At-Risk'
            WHEN e.subscription_end_date <= CURRENT_DATE()
                 AND DATE_DIFF(CURRENT_DATE(), e.subscription_end_date, MONTH) BETWEEN 3 AND 6
            THEN 'Late At-Risk'
        END AS at_risk_stage

    FROM enriched e
    LEFT JOIN accounts a
        ON e.account_id = a.account_id
),

final AS (
    SELECT
        -- PK
        opportunity_id,

        -- At-Risk 정보
        at_risk_stage,
        months_to_end,
        months_since_end,
        opp_mrr,

        -- Opp 기본 정보
        opportunity_name,
        stage_name,
        amount,
        amount_usd,
        currency_code,
        subscription_start_date,
        subscription_end_date,
        contract_type,

        -- Account 정보
        account_id,
        account_name,
        market_segment,
        owner_region,

        -- AE (Opp Owner)
        owner_name AS ae_name,
        owner_email AS ae_email,

        -- CSM
        csm.full_name AS csm_name,
        csm.email AS csm_email

    FROM with_risk
    LEFT JOIN sf_users csm
        ON with_risk.csm_id = csm.sf_user_id
    WHERE at_risk_stage IS NOT NULL
)

SELECT * FROM final