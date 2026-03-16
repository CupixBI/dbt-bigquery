/*
    int_opportunities_enriched.sql
    
    목적: Opportunity에 Account, Owner/Creator, 환율 정보를 enrichment
    
    Grain: Opportunity 1건 = 1행
    
    ※ Team 매칭은 여기서 하지 않음
      - Team은 cost mart에서만 account ↔ team 매핑으로 조인
      - 이유: 1 Account → N Teams 관계로 여기서 조인하면 Opp 뻥튀기 발생
*/

WITH opportunities AS (
    SELECT
        opportunity_id,
        account_id,
        owner_id,
        created_by_id,
        opportunity_name,
        opportunity_type,
        stage_name,
        amount,
        currency_code,
        created_at,
        close_date,
        contract_type,
        contract_term_months,
        subscription_start_date,
        subscription_end_date,
        has_site_insights,
        license_capacity_area,
        license_units,
        probability,
        forecast_category,
        lead_source,
        lead_type,
        win_story,
        loss_reason,
        owner_region,
        opp_number
    FROM {{ ref('stg_salesforce__opportunities') }}
),

accounts AS (
    SELECT * FROM {{ ref('stg_salesforce__accounts') }}
),

sf_users AS (
    SELECT * FROM {{ ref('stg_salesforce__users') }}
),

exchange_rates AS (
    SELECT * FROM {{ source('finance', 'exchange_rate') }}
),

latest_exchange_rates AS (
    SELECT
        currency_code,
        rate_to_usd
    FROM exchange_rates
    WHERE year = (SELECT MAX(year) FROM exchange_rates)
),

final AS (
    SELECT
        -- Opp 기본 정보
        o.opportunity_id,
        o.account_id,
        o.owner_id,
        o.created_by_id,
        o.opportunity_name,
        o.opportunity_type,
        o.stage_name,
        o.amount,
        o.currency_code,
        o.created_at,
        o.close_date,
        o.contract_type,
        o.contract_term_months,
        o.subscription_start_date,
        o.subscription_end_date,
        o.has_site_insights,
        o.license_capacity_area,
        o.license_units,
        o.probability,
        o.forecast_category,
        o.lead_source,
        o.lead_type,
        o.win_story,
        o.loss_reason,
        o.owner_region,
        o.opp_number,

        -- Account 정보
        a.account_name,
        a.industry,
        a.vertical,
        a.market_segment,
        a.territory,
        a.website,
        a.shipping_street,
        a.shipping_city,
        a.shipping_state,
        a.shipping_country,
        a.contract_status,
        a.license_expiration_date,
        a.first_sw_sale_date,

        -- Opp owner/creator 정보
        owner.full_name AS owner_name,
        owner.email AS owner_email,
        creator.full_name AS created_by_name,
        creator.email AS created_by_email,

        -- USD 변환
        COALESCE(er.rate_to_usd, er_latest.rate_to_usd) AS rate_to_usd,
        o.amount * COALESCE(er.rate_to_usd, er_latest.rate_to_usd, 1) AS amount_usd

    FROM opportunities o
    LEFT JOIN accounts a
        ON o.account_id = a.account_id
    LEFT JOIN sf_users owner
        ON o.owner_id = owner.sf_user_id
    LEFT JOIN sf_users creator
        ON o.created_by_id = creator.sf_user_id
    LEFT JOIN exchange_rates er
        ON o.currency_code = er.currency_code
        AND EXTRACT(YEAR FROM o.close_date) = er.year
    LEFT JOIN latest_exchange_rates er_latest
        ON o.currency_code = er_latest.currency_code
)

SELECT * FROM final