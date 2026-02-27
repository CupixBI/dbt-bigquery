/*
    fct_license_usage.sql
    
    목적: 라이센스(quote) 단위 사용 현황 — 대시보드용
    
    그레인: 1행 = 1 quote (= 1 라이센스)
    
    대시보드 컬럼 매핑:
      TEAM          → team_name
      TYPE          → license_label (product_line + billable_type 조합)
      STATUS        → license_status (Active / Expiring / Expired)
      PERIOD        → billing_started_at ~ billing_expires_at
      DAYS LEFT     → days_until_expiry
      CAPACITY USAGE→ used_area_sqft / contracted_area_sqft
      OPP. NUMBER   → opp_number
      OPP. ID       → sf_opportunity_id
      OPP. PERIOD   → subscription_start_date ~ subscription_end_date
      OPP. SIZE     → contracted_area_sqft
      CONTRACT AMT. → opportunity_amount_usd
*/

WITH team_subs AS (
    SELECT * FROM {{ ref('int_team_subscriptions') }}
),

facility_detail AS (
    SELECT * FROM {{ ref('int_facility_details') }}
),

-- 1단계: quote 단위로 집계 (line_items → 1 quote로)
quote_level AS (
    SELECT
        region,
        team_id,
        region_team_id,
        team_name,
        domain,
        team_state,
        team_lock_state,
        account_manager_email,
        primary_csm_email,

        quote_id,
        region_quote_id,
        billable_type,
        billing_started_at,
        billing_expires_at,
        contract_months,
        is_pilot,
        is_currently_billing,
        days_until_expiry,

        -- Workspace 정보 (Team 라이센스면 NULL)
        workspace_id,
        workspace_name,

        -- 라이센스 유형 (quote 내 line_items 중 주력 제품 기준)
        CASE
            WHEN LOGICAL_OR(license_product_line = 'Unified') THEN 'Unified'
            WHEN LOGICAL_OR(license_product_line = 'Builder') THEN 'Builder'
            WHEN LOGICAL_OR(license_product_line = 'Enterprise') THEN 'Enterprise'
            WHEN LOGICAL_OR(license_product_line = 'Basic') THEN 'Basic'
            ELSE 'Unknown'
        END AS product_line,

        -- SiteInsights 포함 여부
        LOGICAL_OR(is_site_insights_product) AS has_site_insights,

        -- 실제 사용 가능 여부 (quote 내 하나라도 actually_usable이면 TRUE)
        LOGICAL_OR(is_actually_usable) AS is_actually_usable,

        -- SF 정보 (opportunity 단위 — quote 내 deduplicate)
        MAX(sf_opportunity_id) AS sf_opportunity_id,
        MAX(opp_number) AS opp_number,
        MAX(CASE WHEN has_sf_opportunity THEN opportunity_amount_usd ELSE 0 END) AS opportunity_amount_usd,
        MAX(CASE WHEN has_sf_opportunity THEN opportunity_mrr ELSE 0 END) AS opportunity_mrr,
        MAX(subscription_start_date) AS subscription_start_date,
        MAX(subscription_end_date) AS subscription_end_date,

        -- SF 계약 면적 (SqFt 통일)
        MAX(CASE
            WHEN has_sf_opportunity AND license_capacity_area IS NOT NULL THEN
                CASE license_units
                    WHEN 'SqFt' THEN license_capacity_area
                    WHEN 'SqM' THEN license_capacity_area * 10.7639
                    WHEN 'Acres' THEN license_capacity_area * 43560
                    ELSE 0
                END
            ELSE 0
        END) AS contracted_area_sqft

    FROM team_subs
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
),

-- 2단계: 실제 사용 면적 — Team 라이센스는 팀 전체, Workspace 라이센스는 해당 workspace만
-- Team 라이센스: 팀의 모든 active facility 면적 합
team_used_area AS (
    SELECT
        team_id,
        region,
        SUM(CASE
            WHEN workspace_lock_state = 'active'
             AND facility_cycle_state = 'created'
             AND facility_size_unit = 'SQFT' THEN facility_size
            WHEN workspace_lock_state = 'active'
             AND facility_cycle_state = 'created'
             AND facility_size_unit = 'SQM' THEN facility_size * 10.7639
            ELSE 0
        END) AS used_area_sqft
    FROM facility_detail
    GROUP BY 1, 2
),

-- Workspace 라이센스: 해당 workspace의 active facility 면적 합
workspace_used_area AS (
    SELECT
        workspace_id,
        region,
        SUM(CASE
            WHEN workspace_lock_state = 'active'
             AND facility_cycle_state = 'created'
             AND facility_size_unit = 'SQFT' THEN facility_size
            WHEN workspace_lock_state = 'active'
             AND facility_cycle_state = 'created'
             AND facility_size_unit = 'SQM' THEN facility_size * 10.7639
            ELSE 0
        END) AS used_area_sqft
    FROM facility_detail
    GROUP BY 1, 2
),

-- 최종
final AS (
    SELECT
        ql.region,
        ql.team_id,
        ql.region_team_id,
        ql.team_name,
        ql.domain,
        ql.team_state,
        ql.team_lock_state,
        ql.account_manager_email,
        ql.primary_csm_email,

        ql.quote_id,
        ql.region_quote_id,
        ql.billable_type,
        ql.billing_started_at,
        ql.billing_expires_at,
        ql.contract_months,
        ql.is_pilot,
        ql.is_currently_billing,
        ql.is_actually_usable,
        ql.days_until_expiry,

        ql.workspace_id,
        ql.workspace_name,

        -- TYPE: product_line + billable_type 조합
        ql.product_line,
        ql.billable_type AS license_scope,
        CONCAT(ql.product_line, ' - ', ql.billable_type) AS license_label,
        ql.has_site_insights,

        -- STATUS (is_actually_usable 기반: billing 중 + 제품 active)
        CASE
            WHEN NOT ql.is_actually_usable AND NOT ql.is_currently_billing THEN 'Expired'
            WHEN NOT ql.is_actually_usable AND ql.is_currently_billing THEN 'Inactive'
            WHEN ql.is_actually_usable AND ql.days_until_expiry <= 30 THEN 'Expiring'
            WHEN ql.is_actually_usable THEN 'Active'
            ELSE 'Unknown'
        END AS license_status,

        -- SF 정보
        ql.sf_opportunity_id,
        ql.opp_number,
        ql.opportunity_amount_usd,
        ql.opportunity_mrr,
        ql.subscription_start_date,
        ql.subscription_end_date,

        -- 면적
        ql.contracted_area_sqft,
        CASE
            WHEN ql.billable_type = 'Team' THEN COALESCE(tua.used_area_sqft, 0)
            WHEN ql.billable_type = 'Workspace' THEN COALESCE(wua.used_area_sqft, 0)
            ELSE 0
        END AS used_area_sqft,
        SAFE_DIVIDE(
            CASE
                WHEN ql.billable_type = 'Team' THEN COALESCE(tua.used_area_sqft, 0)
                WHEN ql.billable_type = 'Workspace' THEN COALESCE(wua.used_area_sqft, 0)
                ELSE 0
            END,
            NULLIF(ql.contracted_area_sqft, 0)
        ) AS capacity_utilization_rate

    FROM quote_level ql

    LEFT JOIN team_used_area tua
        ON ql.billable_type = 'Team'
        AND ql.team_id = tua.team_id
        AND ql.region = tua.region

    LEFT JOIN workspace_used_area wua
        ON ql.billable_type = 'Workspace'
        AND ql.workspace_id = wua.workspace_id
        AND ql.region = wua.region
)

SELECT * FROM final