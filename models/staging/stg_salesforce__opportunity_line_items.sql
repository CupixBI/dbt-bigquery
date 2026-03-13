/*
    stg_sf__opportunity_line_items.sql
    
    목적: Salesforce OpportunityLineItem 데이터 정리 (제품별 상세)
    
    처리:
      - 스냅샷 중복 제거 (_extracted_at 기준 최신만)
      - 삭제된 레코드 제외 (_is_deleted)
      - 제품/매출 관련 핵심 필드 선별
*/

WITH source AS (
    SELECT * FROM {{ source('salesforce', 'opportunitylineitem') }}
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
        Id AS line_item_id,

        -- 관계 키
        OpportunityId AS opportunity_id,
        Product2Id AS product_id,

        -- 제품 정보
        Name AS product_name,
        ProductCode AS product_code,
        COALESCE(Family__c, 'Unknown') AS product_family,
        COALESCE(Reporting_Category__c, 'Unknown') AS reporting_category,
        Description__c AS description,

        -- 금액
        CAST(Quantity AS FLOAT64) AS quantity,
        CAST(UnitPrice AS FLOAT64) AS unit_price,
        CAST(ListPrice AS FLOAT64) AS list_price,
        CAST(TotalPrice AS FLOAT64) AS total_price,
        CAST(Discount__c AS FLOAT64) AS discount_pct,
        CAST(MSRP_Line_Total__c AS FLOAT64) AS msrp_line_total,
        CAST(Line_COGS__c AS FLOAT64) AS line_cogs,

        -- 라이센스/구독
        CAST(License_Capacity__c AS FLOAT64) AS license_capacity,
        Units__c AS license_units,
        CAST(Subscription_Term__c AS FLOAT64) AS subscription_term,
        DATE(ServiceDate) AS service_date,

        -- 메타
        TIMESTAMP(_extracted_at) AS extracted_at

    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM renamed