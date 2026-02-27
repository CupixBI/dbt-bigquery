/*
    int_team_revenue.sql
    
    목적: 팀(=회사) 단위 매출 집계 (intermediate - mart의 재료)
    
    주의: 하나의 SF opportunity가 여러 line_items에 걸릴 수 있음
      - 같은 quote 내 여러 product (라이센스 + Add-on)
      - 같은 opp가 여러 workspace에 걸림
      → opportunity 단위로 deduplicate하여 매출 중복 합산 방지
    
    그레인: team (1행 = 1팀)
*/

WITH team_subs AS (
    SELECT * FROM {{ ref('int_team_subscriptions') }}
),

-- 1단계: 팀별 구독 요약 (quote 레벨)
team_quotes AS (
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

        -- quote 수
        COUNT(DISTINCT quote_id) AS total_quotes,
        COUNT(DISTINCT CASE WHEN is_currently_billing THEN quote_id END) AS active_quotes,

        -- billable_type별 quote 수
        COUNT(DISTINCT CASE WHEN billable_type = 'Team' THEN quote_id END) AS team_license_quotes,
        COUNT(DISTINCT CASE WHEN billable_type = 'Workspace' THEN quote_id END) AS workspace_license_quotes,

        -- workspace 수
        COUNT(DISTINCT workspace_id) AS total_workspaces,

        -- 만료 관련
        MIN(CASE WHEN is_currently_billing THEN days_until_expiry END) AS earliest_expiry_days,
        MAX(billing_expires_at) AS latest_billing_expires_at,

        -- SF 연결 현황
        COUNT(DISTINCT CASE WHEN has_sf_opportunity THEN sf_opportunity_id END) AS sf_connected_opportunities,
        COUNT(DISTINCT CASE WHEN NOT has_sf_opportunity THEN line_item_id END) AS sf_unconnected_line_items

    FROM team_subs
    GROUP BY 1,2,3,4,5,6,7,8,9
),

-- 2단계: 팀별 매출 (opportunity 단위 deduplicate)
team_revenue AS (
    SELECT
        team_id,
        region,

        -- opportunity 단위로 deduplicate하여 합산
        SUM(opportunity_amount_usd) AS total_revenue_usd,
        SUM(opportunity_mrr) AS total_mrr,
        COUNT(*) AS total_opportunities,
        SUM(CASE WHEN opportunity_is_won THEN opportunity_amount_usd ELSE 0 END) AS won_revenue_usd,
        COUNT(CASE WHEN opportunity_is_won THEN 1 END) AS won_opportunities

    FROM (
        -- 같은 team + 같은 opportunity → 1행만
        SELECT DISTINCT
            team_id,
            region,
            sf_opportunity_id,
            opportunity_amount_usd,
            opportunity_mrr,
            opportunity_is_won
        FROM team_subs
        WHERE has_sf_opportunity = TRUE
    )
    GROUP BY 1,2
),

-- 2-1단계: 팀별 SF 계약 면적 (현재 billing 중 + opportunity 단위 deduplicate, SqFt 통일)
team_contracted_area AS (
    SELECT
        team_id,
        region,
        SUM(capacity_sqft) AS contracted_area_sqft
    FROM (
        SELECT DISTINCT
            team_id,
            region,
            sf_opportunity_id,
            CASE license_units
                WHEN 'SqFt' THEN license_capacity_area
                WHEN 'SqM' THEN license_capacity_area * 10.7639
                WHEN 'Acres' THEN license_capacity_area * 43560
                ELSE 0  -- CupixVista Credits 등 면적 아닌 것 제외
            END AS capacity_sqft
        FROM team_subs
        WHERE has_sf_opportunity = TRUE
          AND is_currently_billing = TRUE
          AND license_capacity_area IS NOT NULL
    )
    GROUP BY 1, 2
),

-- 3단계: 팀별 라이센스 유형 (product_name 기반 — 100% 커버, 현재 billing 중인 것만)
team_license_types AS (
    SELECT
        team_id,
        region,

        -- 제품 라인 요약 (우선순위: Unified > Builder > Enterprise > Basic)
        CASE
            WHEN LOGICAL_OR(license_product_line = 'Unified') THEN 'Unified'
            WHEN LOGICAL_OR(license_product_line = 'Builder') THEN 'Builder'
            WHEN LOGICAL_OR(license_product_line = 'Enterprise') THEN 'Enterprise'
            WHEN LOGICAL_OR(license_product_line = 'Basic') THEN 'Basic'
            ELSE 'Unknown'
        END AS primary_product_line,

        -- SiteInsights
        LOGICAL_OR(is_site_insights_product) AS has_site_insights

    FROM team_subs
    WHERE is_currently_billing = TRUE
    GROUP BY 1, 2
),

-- 4단계: 라이센스 스코프 (가장 최근 quote 기준 — Team 또는 Workspace 하나만)
team_license_scope AS (
    SELECT
        team_id,
        region,
        billable_type AS license_type
    FROM (
        SELECT
            team_id,
            region,
            billable_type,
            ROW_NUMBER() OVER (
                PARTITION BY team_id, region
                ORDER BY billing_started_at DESC
            ) AS rn
        FROM team_subs
    )
    WHERE rn = 1
),

-- 최종 조합
final AS (
    SELECT
        tq.region,
        tq.team_id,
        tq.region_team_id,
        tq.team_name,
        tq.domain,
        tq.team_state,
        tq.team_lock_state,
        tq.account_manager_email,
        tq.primary_csm_email,

        -- 구독 현황
        tq.total_quotes,
        tq.active_quotes,
        tq.team_license_quotes,
        tq.workspace_license_quotes,
        tq.total_workspaces,
        tq.earliest_expiry_days,
        tq.latest_billing_expires_at,

        -- SF 연결 현황
        tq.sf_connected_opportunities,
        tq.sf_unconnected_line_items,

        -- 매출 (opportunity 단위 deduplicated)
        COALESCE(tr.total_revenue_usd, 0) AS total_revenue_usd,
        COALESCE(tr.total_mrr, 0) AS total_mrr,
        COALESCE(tr.total_opportunities, 0) AS total_opportunities,
        COALESCE(tr.won_revenue_usd, 0) AS won_revenue_usd,
        COALESCE(tr.won_opportunities, 0) AS won_opportunities,

        -- 라이센스 유형 (product_name 기반, 현재 billing 중인 것만)
        COALESCE(tlt.primary_product_line, 'Unknown') AS primary_product_line,
        COALESCE(tlt.has_site_insights, FALSE) AS has_site_insights,

        -- 라이센스 스코프 (가장 최근 quote 기준 — Team 또는 Workspace)
        tls.license_type,

        -- SF 계약 면적 (현재 billing 중, SqFt 통일)
        COALESCE(tca.contracted_area_sqft, 0) AS contracted_area_sqft

    FROM team_quotes tq
    LEFT JOIN team_revenue tr
        ON tq.team_id = tr.team_id AND tq.region = tr.region
    LEFT JOIN team_contracted_area tca
        ON tq.team_id = tca.team_id AND tq.region = tca.region
    LEFT JOIN team_license_types tlt
        ON tq.team_id = tlt.team_id AND tq.region = tlt.region
    LEFT JOIN team_license_scope tls
        ON tq.team_id = tls.team_id AND tq.region = tls.region
)

SELECT * FROM final