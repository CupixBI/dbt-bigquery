/*
    stg_salesforce__leads.sql
    
    목적: Salesforce Lead 데이터 정리
    
    처리:
      - 스냅샷 중복 제거 (_extracted_at 기준 최신만)
      - 삭제된 레코드 제외 (_is_deleted)
      - 핵심 필드 선별 및 이름 정리
*/

WITH source AS (
    SELECT * FROM {{ source('salesforce', 'lead') }}
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
        Id AS lead_id,

        -- 기본 정보
        Name AS lead_name,
        Email AS email,
        Company AS company,
        COALESCE(Title, 'Unknown') AS title,
        COALESCE(Status, 'Unknown') AS status,
        COALESCE(Rating, 'Unknown') AS rating,
        COALESCE(Industry, 'Unknown') AS industry,

        -- 분류
        COALESCE(Market_Segmentation__c, 'Unknown') AS market_segment,
        COALESCE(Lead_Type__c, 'Unknown') AS lead_type,
        COALESCE(Firm_Type__c, 'Unknown') AS firm_type,
        COALESCE(Job_Type__c, 'Unknown') AS job_type,
        COALESCE(Owner_Region__c, 'Unknown') AS owner_region,

        -- 소스
        COALESCE(LeadSource, 'Unknown') AS lead_source,
        Original_Source__c AS original_source,
        Latest_Source__c AS latest_source,

        -- 영업 활동
        CAST(Touches__c AS INT64) AS touches,
        DATE(LastActivityDate) AS last_activity_date,

        -- 전환 정보
        CAST(IsConverted AS BOOLEAN) AS is_converted,
        ConvertedDate AS converted_date,
        ConvertedAccountId AS converted_account_id,
        ConvertedContactId AS converted_contact_id,
        ConvertedOpportunityId AS converted_opportunity_id,

        -- 날짜
        TIMESTAMP(CreatedDate) AS created_at,
        DATE(CreatedDate) AS created_date,

        -- Owner
        OwnerId AS owner_id,

        -- 메타
        TIMESTAMP(_extracted_at) AS extracted_at

    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM renamed