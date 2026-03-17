/*
    mart_leads.sql
    
    목적: Leads 대시보드용 Lead 상세 테이블
    
    Grain: Lead 1건 = 1행
    
    용도:
      - Total Leads / Converted Leads 집계
      - Status, Source, Region, Org Segment, Owner별 분석
      - Monthly Lead/Converted Trend
      - Lead 상세 테이블
*/

WITH leads AS (
    SELECT * FROM {{ ref('stg_salesforce__leads') }}
),

sf_users AS (
    SELECT
        sf_user_id,
        full_name,
        email
    FROM {{ ref('stg_salesforce__users') }}
),

final AS (
    SELECT
        -- PK
        l.lead_id,

        -- 기본 정보
        l.lead_name,
        l.email AS lead_email,
        l.company,
        l.title,
        l.status,
        l.rating,
        l.industry,

        -- 분류
        l.market_segment,
        l.lead_type,
        l.firm_type,
        l.job_type,
        l.owner_region,

        -- 소스
        l.lead_source,
        l.original_source,
        l.latest_source,

        -- 영업 활동
        l.touches,
        l.last_activity_date,
        DATE_DIFF(CURRENT_DATE(), l.last_activity_date, DAY) AS days_since_last_activity,

        -- 전환 정보
        l.is_converted,
        l.converted_date,
        l.converted_account_id,
        l.converted_contact_id,
        l.converted_opportunity_id,

        -- 전환 소요일
        CASE
            WHEN l.is_converted = TRUE
            THEN DATE_DIFF(l.converted_date, l.created_date, DAY)
        END AS days_to_conversion,

        -- 날짜
        l.created_at,
        l.created_date,
        EXTRACT(YEAR FROM l.created_date) AS created_year,
        EXTRACT(MONTH FROM l.created_date) AS created_month,
        FORMAT_DATE('%Y-%m', l.created_date) AS created_year_month,

        -- Owner
        l.owner_id,
        u.full_name AS owner_name,
        u.email AS owner_email

    FROM leads l
    LEFT JOIN sf_users u
        ON l.owner_id = u.sf_user_id
)

SELECT * FROM final