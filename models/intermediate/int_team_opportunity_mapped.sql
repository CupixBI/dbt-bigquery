-- int_team_opportunity_mapped.sql

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

teams AS (
    SELECT * FROM {{ ref('stg_tesla__teams') }}
),

seed_mapping AS (
    SELECT * FROM {{ ref('stg_seed__team_sf_account_mapping') }}
),

sf_users AS (
    SELECT * FROM {{ ref('stg_salesforce__users') }}
),

accounts AS (
    SELECT * FROM {{ ref('stg_salesforce__accounts') }}
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

-- 1순위: teams.sf_resource_id로 직접 매칭
matched_by_sf_resource AS (
    SELECT
        o.*,
        t.region AS team_region,
        t.region_team_id,
        t.team_id,
        t.team_name,
        t.account_manager_id,
        t.account_manager_email,
        t.primary_csm_id,
        t.primary_csm_email,
        t.secondary_csm_id,
        t.secondary_csm_email,
        'sf_resource_id' AS match_source
    FROM opportunities o
    INNER JOIN teams t
        ON o.account_id = t.sf_resource_id
),

-- 1순위 매칭 안 된 Opp
unmatched_after_1 AS (
    SELECT o.*
    FROM opportunities o
    LEFT JOIN teams t
        ON o.account_id = t.sf_resource_id
    WHERE t.sf_resource_id IS NULL
),

-- 2순위: seed CSV의 sf_account_id로 매칭
matched_by_seed AS (
    SELECT
        u.*,
        t.region AS team_region,
        t.region_team_id,
        t.team_id,
        t.team_name,
        t.account_manager_id,
        t.account_manager_email,
        t.primary_csm_id,
        t.primary_csm_email,
        t.secondary_csm_id,
        t.secondary_csm_email,
        'seed_mapping' AS match_source
    FROM unmatched_after_1 u
    INNER JOIN seed_mapping s
        ON u.account_id = s.sf_account_id
    LEFT JOIN teams t
        ON s.region_team_id = t.region_team_id
),

-- 매칭 안 된 Opp
still_unmatched AS (
    SELECT
        u.*,
        CAST(NULL AS STRING) AS team_region,
        CAST(NULL AS STRING) AS region_team_id,
        CAST(NULL AS STRING) AS team_id,
        CAST(NULL AS STRING) AS team_name,
        CAST(NULL AS STRING) AS account_manager_id,
        CAST(NULL AS STRING) AS account_manager_email,
        CAST(NULL AS STRING) AS primary_csm_id,
        CAST(NULL AS STRING) AS primary_csm_email,
        CAST(NULL AS STRING) AS secondary_csm_id,
        CAST(NULL AS STRING) AS secondary_csm_email,
        'unmatched' AS match_source
    FROM unmatched_after_1 u
    LEFT JOIN seed_mapping s
        ON u.account_id = s.sf_account_id
    WHERE s.sf_account_id IS NULL
),

combined AS (
    SELECT * FROM matched_by_sf_resource
    UNION ALL
    SELECT * FROM matched_by_seed
    UNION ALL
    SELECT * FROM still_unmatched
),

final AS (
    SELECT
        c.*,
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
        c.amount * COALESCE(er.rate_to_usd, er_latest.rate_to_usd, 1) AS amount_usd
    FROM combined c
    LEFT JOIN accounts a
        ON c.account_id = a.account_id
    LEFT JOIN sf_users owner
        ON c.owner_id = owner.sf_user_id
    LEFT JOIN sf_users creator
        ON c.created_by_id = creator.sf_user_id
    LEFT JOIN exchange_rates er
        ON c.currency_code = er.currency_code
        AND EXTRACT(YEAR FROM c.close_date) = er.year
    LEFT JOIN latest_exchange_rates er_latest
        ON c.currency_code = er_latest.currency_code
)

SELECT * FROM final