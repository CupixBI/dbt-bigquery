/*
    stg_salesforce__opportunities.sql
    
    목적: Salesforce Opportunity 데이터 정리
    
    처리:
      - 스냅샷 중복 제거 (_extracted_at 기준 최신만)
      - 삭제된 레코드 제외 (_is_deleted)
      - 매출/계약 관련 핵심 필드만 선별
      - 타입 변환 및 이름 정리
*/

WITH source AS (
    SELECT * FROM {{ source('salesforce', 'opportunity') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY Id
            ORDER BY _extracted_at DESC
        ) AS row_num
    FROM source
    WHERE _is_deleted = FALSE
),

renamed AS (
    SELECT
        -- PK
        Id AS opportunity_id,
        
        -- 관계 키
        AccountId AS account_id,
        OwnerId AS owner_id,
        CreatedById AS created_by_id,

        -- 기본 정보
        Name AS opportunity_name,
        Type AS opportunity_type,
        StageName AS stage_name,
        CAST(IsClosed AS BOOLEAN) AS is_closed,
        CAST(IsWon AS BOOLEAN) AS is_won,

        -- 매출
        CAST(Amount AS FLOAT64) AS amount,
        CAST(Amount_USD__c AS FLOAT64) AS amount_usd,
        CurrencyIsoCode AS currency_code,
        CAST(MRR__c AS FLOAT64) AS mrr,

        -- 날짜
        TIMESTAMP(CreatedDate) AS created_at,
        DATE(CloseDate) AS close_date,

        -- 계약/구독 정보
        COALESCE(Contract_Type__c, 'Unknown') AS contract_type,
        CAST(Site_Insights__c AS BOOLEAN) AS has_site_insights,
        DATE(Subscription_Start__c) AS subscription_start_date,
        DATE(Subscription_End__c) AS subscription_end_date,
        CAST(Contract_Term_months__c AS INT64) AS contract_term_months,

        -- 라이센스
        CAST(License_Capacity_Area__c AS FLOAT64) AS license_capacity_area,
        Units__c AS license_units,

        -- 파이프라인/예측
        CAST(Probability AS FLOAT64) AS probability,
        COALESCE(ForecastCategoryName, 'Unknown') AS forecast_category,

        -- 리드/마케팅
        COALESCE(LeadSource, 'Unknown') AS lead_source,
        COALESCE(Lead_Type__c, 'Unknown') AS lead_type,

        -- Win/Loss 분석
        Win_Story__c AS win_story,
        COALESCE(Lost_Reason__c, Loss_Reason__c, 'Unknown') AS loss_reason,

        -- Region
        COALESCE(Owner_Region__c, 'Unknown') AS owner_region,

        -- OPP 번호
        OPP_Number__c AS opp_number,

        -- 메타
        TIMESTAMP(_extracted_at) AS extracted_at

    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM renamed