/*
    int_line_items_enriched.sql
    
    목적: line_items를 정리하고, 유효 quotes + SF 매출 + 활성화 상태를 연결
    
    SF 정보 원칙:
      - line_items의 SF 필드는 생성 시점 스냅샷이라 이후 SF 업데이트가 반영 안 됨
      - 따라서 SF 정보는 모두 stg_sf_opportunities에서 가져옴 (Single Source of Truth)
      - line_items에서는 sf_opportunity_id (조인 키)만 사용
    
    처리 로직:
      - disabled_at이 있는 line_items 제외
      - int_quotes_classified와 INNER JOIN → 잡음 quote의 line_items 자동 제외
      - SF opportunity 조인 → 매출/계약/라이센스 정보 연결 (없으면 0 또는 Unknown)
      - applied_products 조인 → 실제 제품 활성화 여부 (is_product_active)
*/

WITH line_items AS (
    SELECT * FROM {{ ref('stg_tesla__line_items') }}
),

quotes_classified AS (
    SELECT * FROM {{ ref('int_quotes_classified') }}
),

sf_opportunities AS (
    SELECT * FROM {{ ref('stg_salesforce__opportunities') }}
),

applied_products AS (
    SELECT * FROM {{ ref('stg_tesla__applied_products') }}
),

-- 1단계: disabled 제외
active_line_items AS (
    SELECT *
    FROM line_items
    WHERE disabled_at IS NULL
),

-- 2단계: quotes 분류 정보 연결
with_quotes AS (
    SELECT
        li.line_item_id,
        li.quote_id,
        li.product_id,
        li.product_name,
        li.product_type,
        li.region,
        li.line_item_name,
        li.created_at AS line_item_created_at,

        -- SF 조인 키만 사용
        li.sf_opportunity_id,

        -- quotes 분류 정보
        qc.billable_id,
        qc.region_billable_id,
        qc.billable_type,
        qc.billing_started_at AS quote_billing_started_at,
        qc.billing_expires_at AS quote_billing_expires_at,
        qc.contract_months,
        qc.quote_name,
        qc.is_pilot

    FROM active_line_items li
    INNER JOIN quotes_classified qc
        ON li.quote_id = qc.quote_id
        AND li.region = qc.region
),

-- 3단계: SF opportunity + applied_products 조인
joined AS (
    SELECT
        wq.*,

        -- SF 연결 여부
        CASE
            WHEN wq.sf_opportunity_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_sf_opportunity,

        -- SF 매출 (미연결 시 0)
        COALESCE(opp.amount_usd, 0) AS opportunity_amount_usd,
        COALESCE(opp.mrr, 0) AS opportunity_mrr,
        opp.currency_code AS opportunity_currency_code,
        opp.amount AS opportunity_amount_original,

        -- SF Opportunity 상태
        opp.stage_name AS opportunity_stage,
        COALESCE(opp.is_won, FALSE) AS opportunity_is_won,
        COALESCE(opp.is_closed, FALSE) AS opportunity_is_closed,
        opp.opportunity_type,
        opp.close_date AS opportunity_close_date,

        -- SF 계약 정보 (참고용)
        opp.contract_type AS sf_contract_type,
        opp.subscription_start_date,
        opp.subscription_end_date,
        opp.contract_term_months,
        opp.license_capacity_area,
        opp.license_units,
        opp.account_id AS sf_account_id,
        opp.opp_number,

        -- SF Owner (영업 담당)
        opp.owner_id AS opportunity_owner_id,

        -- 제품 활성화 여부 (applied_products 기반)
        CASE
            WHEN ap.applied_state = 'active' THEN TRUE
            ELSE FALSE
        END AS is_product_active,

        -- 라이센스 유형 분류 (product_name 기반 — region별 정확한 매핑)
        CASE
            WHEN wq.product_name = 'Unified Platform' THEN 'Unified'
            WHEN wq.product_name = 'Core Builder' THEN 'Builder'
            WHEN wq.product_name LIKE 'Basic Subscription%' THEN 'Basic'
            WHEN wq.product_name LIKE 'Enterprise Subscription%' THEN 'Enterprise'
            WHEN wq.product_name LIKE 'Unlimited Subscription%' THEN 'Enterprise'
            WHEN wq.product_name = 'SiteInsights Add-On' THEN 'SiteInsights'
            WHEN wq.product_name = 'Site Insights Pack' THEN 'SiteInsights'
            ELSE 'Other'
        END AS license_product_line,

        -- SiteInsights 여부
        CASE
            WHEN wq.product_name IN ('SiteInsights Add-On', 'Site Insights Pack') THEN TRUE
            ELSE FALSE
        END AS is_site_insights_product

    FROM with_quotes wq

    LEFT JOIN sf_opportunities opp
        ON wq.sf_opportunity_id = opp.opportunity_id

    LEFT JOIN applied_products ap
        ON wq.quote_id = ap.quote_id
        AND wq.product_id = ap.product_id
        AND wq.region = ap.region
)

SELECT * FROM joined