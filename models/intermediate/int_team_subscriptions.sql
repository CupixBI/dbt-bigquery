/*
    int_team_subscriptions.sql
    
    목적: 유효 quote를 팀(=회사) 단위로 롤업, SF 매출 및 영업 담당 정보 포함
    
    핵심 로직:
      - Team 라이센스: billable_id = team_id (직접 매핑)
      - Workspace 라이센스: billable_id = workspace_id → workspaces.team_id로 팀 특정
      - line_items를 통해 SF opportunity 매출 + owner 연결
    
    결과: 팀 × quote × line_item 레벨의 팩트 테이블
*/

WITH quotes_classified AS (
    SELECT * FROM {{ ref('int_quotes_classified') }}
),

line_items_enriched AS (
    SELECT * FROM {{ ref('int_line_items_enriched') }}
),

teams AS (
    SELECT * FROM {{ ref('stg_tesla__teams') }}
),

workspaces AS (
    SELECT * FROM {{ ref('stg_tesla__workspaces') }}
),

sf_users AS (
    SELECT * FROM {{ ref('stg_salesforce__users') }}
),

-- 1단계: quote에 team_id 통일
quotes_with_team AS (
    SELECT
        qc.*,
        
        CASE
            WHEN qc.billable_type = 'Team' THEN qc.billable_id
            WHEN qc.billable_type = 'Workspace' THEN w.team_id
        END AS team_id,

        CASE
            WHEN qc.billable_type = 'Team' THEN qc.region_billable_id
            WHEN qc.billable_type = 'Workspace' THEN w.region_team_id
        END AS region_team_id,

        w.workspace_id,
        w.region_workspace_id,
        w.workspace_name

    FROM quotes_classified qc

    LEFT JOIN workspaces w
        ON qc.billable_type = 'Workspace'
        AND qc.billable_id = w.workspace_id
        AND qc.region = w.region
),

-- 2단계: 팀 정보 붙이기
with_team_info AS (
    SELECT
        qt.region,
        
        -- 팀 정보
        qt.team_id,
        qt.region_team_id,
        t.team_name,
        t.domain,
        t.state AS team_state,
        t.lock_state AS team_lock_state,
        t.account_manager_email,
        t.primary_csm_email,

        -- Quote 정보
        qt.quote_id,
        qt.region_quote_id,
        qt.billable_type,
        qt.billing_started_at,
        qt.billing_expires_at,
        qt.contract_months,
        qt.quote_name,
        qt.is_pilot,

        -- Workspace 정보
        qt.workspace_id,
        qt.region_workspace_id,
        qt.workspace_name

    FROM quotes_with_team qt

    LEFT JOIN teams t
        ON qt.team_id = t.team_id
        AND qt.region = t.region
),

-- 3단계: line_items + SF 매출 + owner 붙이기
final AS (
    SELECT
        ti.region,
        
        -- 팀(회사) 정보
        ti.team_id,
        ti.region_team_id,
        ti.team_name,
        ti.domain,
        ti.team_state,
        ti.team_lock_state,
        ti.account_manager_email,
        ti.primary_csm_email,

        -- Quote 정보
        ti.quote_id,
        ti.region_quote_id,
        ti.billable_type,
        ti.billing_started_at,
        ti.billing_expires_at,
        ti.contract_months,
        ti.is_pilot,

        -- billing 상태 플래그 (매 빌드 시 현재 시점 기준으로 계산)
        CASE
            WHEN ti.billing_started_at <= CURRENT_TIMESTAMP()
                 AND ti.billing_expires_at > CURRENT_TIMESTAMP()
            THEN TRUE
            ELSE FALSE
        END AS is_currently_billing,

        DATE_DIFF(
            DATE(ti.billing_expires_at),
            CURRENT_DATE(),
            DAY
        ) AS days_until_expiry,

        -- Workspace 정보
        ti.workspace_id,
        ti.region_workspace_id,
        ti.workspace_name,

        -- Line Item 정보
        li.line_item_id,
        li.product_id,
        li.product_name,

        -- 라이센스 분류 (product_name 기반 — 100% 커버)
        li.license_product_line,
        li.is_site_insights_product,
        li.product_type,

        -- 제품 활성화 상태
        li.is_product_active,

        -- 실제 사용 가능 여부 (billing 중 + 제품 활성화)
        CASE
            WHEN ti.billing_started_at <= CURRENT_TIMESTAMP()
                 AND ti.billing_expires_at > CURRENT_TIMESTAMP()
                 AND li.is_product_active = TRUE
            THEN TRUE
            ELSE FALSE
        END AS is_actually_usable,

        -- SF 연결 정보
        li.has_sf_opportunity,
        li.sf_opportunity_id,
        li.sf_account_id,
        li.sf_contract_type,
        li.opp_number,
        li.license_capacity_area,
        li.license_units,
        li.subscription_start_date,
        li.subscription_end_date,
        li.contract_term_months,

        -- SF 매출 정보
        li.opportunity_amount_usd,
        li.opportunity_mrr,
        li.opportunity_currency_code,
        li.opportunity_amount_original,

        -- SF Opportunity 상태
        li.opportunity_stage,
        li.opportunity_is_won,
        li.opportunity_is_closed,
        li.opportunity_type,
        li.opportunity_close_date,

        -- 영업 담당자 (SF Opportunity Owner)
        li.opportunity_owner_id,
        sf_owner.full_name AS opportunity_owner_name,
        sf_owner.email AS opportunity_owner_email

    FROM with_team_info ti

    LEFT JOIN line_items_enriched li
        ON ti.quote_id = li.quote_id
        AND ti.region = li.region

    LEFT JOIN sf_users sf_owner
        ON li.opportunity_owner_id = sf_owner.sf_user_id
)

SELECT * FROM final