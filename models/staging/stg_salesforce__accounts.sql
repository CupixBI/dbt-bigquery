/*
    stg_salesforce__accounts.sql
    
    목적: Salesforce Account 데이터 정리
    
    처리:
      - 스냅샷 중복 제거 (_extracted_at 기준 최신만)
      - 삭제된 레코드 제외 (_is_deleted)
      - 핵심 필드 선별 및 이름 정리
*/

WITH source AS (
    SELECT * FROM {{ source('salesforce', 'account') }}
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
        Id AS account_id,

        -- 기본 정보
        Name AS account_name,
        COALESCE(Industry, 'Unknown') AS industry,
        COALESCE(Vertical__c, 'Unknown') AS vertical,
        COALESCE(Market_Segment__c, 'Unknown') AS market_segment,
        Website AS website,

        -- 배송 주소
        ShippingStreet AS shipping_street,
        ShippingCity AS shipping_city,
        ShippingState AS shipping_state,
        ShippingPostalCode AS shipping_postal_code,
        ShippingCountry AS shipping_country,

        -- 영업 테리토리
        COALESCE(Territory__c, 'Unknown') AS territory,

        -- 계약/라이센스
        COALESCE(Contract_Status__c, 'Unknown') AS contract_status,
        DATE(License_Expiration_for_Account__c) AS license_expiration_date,

        -- 첫 거래일
        DATE(X1st_SW_Sale_Date__c) AS first_sw_sale_date,

        -- 관계 키
        OwnerId AS owner_id,
        ParentId AS parent_account_id,

        -- 메타
        TIMESTAMP(_extracted_at) AS extracted_at

    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM renamed