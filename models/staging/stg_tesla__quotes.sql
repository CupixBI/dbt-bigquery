WITH source AS (
    SELECT * FROM {{ source('tesla', 'quotes') }}
),

renamed AS(
    SELECT
        CAST(_id AS STRING) AS quote_id,
        region,
        CAST(billable_id AS STRING) AS billable_id,
        billable_type,
        
        -- [추천] 안전한 연산을 위해 명시적으로 타입 변환
        TIMESTAMP(billing_started_at) AS billing_started_at,
        CAST(contract_months AS INT64) AS contract_months,
        TIMESTAMP(created_at) AS created_at,
        name AS quote_name,
        pilot,
        quote_type,
        state,
        CAST(user_id AS STRING) AS created_by_user_id,

        -- [핵심 추가 로직] billing_expired_at 생성
        CASE 
            -- 1200개월(100년)이 넘는 비정상 데이터는 에러 방지를 위해 처리
            WHEN CAST(contract_months AS INT64) > 1200 THEN '2999-12-31 23:59:59' 
            ELSE TIMESTAMP(
                DATETIME_ADD(
                    DATETIME(TIMESTAMP(billing_started_at)), 
                    INTERVAL CAST(contract_months AS INT64) MONTH
                )
            )
        END AS billing_expires_at,

        tenant

    FROM source
),

final AS (
    SELECT
        region,
        quote_id,
        tenant,

        -- Region Prefix (다른 테이블들과의 조인 키)
        CONCAT(
            CASE region
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-',
            quote_id,
            '-',
            tenant
        ) AS region_quote_id,
        
        billable_id,
        CONCAT(
            CASE region
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-',
            billable_id,
            '-',
            tenant
        ) AS region_billable_id,

        billable_type,
        billing_started_at,
        billing_expires_at, -- [추가된 컬럼]
        contract_months,
        
        -- Null 처리 및 기본값 처리        
        created_at,
        COALESCE(quote_name, 'Unknown') AS quote_name,
        COALESCE(pilot, FALSE) AS is_pilot,
        COALESCE(quote_type, 'Unknown') AS quote_type,
        COALESCE(state, 'Unknown') AS state,
        
        created_by_user_id,
        CONCAT(
            CASE region
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-',
            created_by_user_id,
            '-',
            tenant
        ) AS region_created_by_user_id

    FROM renamed
)

SELECT * FROM final