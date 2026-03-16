/*
    stg_sf_users.sql
    
    목적: Salesforce User 데이터 정리 (Opportunity Owner/Creator 추적용)
    
    처리:
      - 스냅샷 중복 제거 (_extracted_at 기준 최신만)
      - 핵심 필드만 선별
*/

WITH source AS (
    SELECT * FROM {{ source('salesforce', 'user') }}
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
        Id AS sf_user_id,
        Name AS full_name,
        FirstName AS first_name,
        LastName AS last_name,
        Email AS email,
        CAST(IsActive AS BOOLEAN) AS is_active,
        COALESCE(Title, 'Unknown') AS title,
        COALESCE(Department, 'Unknown') AS department,
        COALESCE(Region__c, 'Unknown') AS region

    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM renamed