/*
    stg_products.sql
    
    목적: 제품(product) 마스터 테이블
    
    주의: 같은 product_id가 region별로 다른 제품을 의미함!
      예: id=18 → uswe2: Enterprise Subscription for Workspace
                  apse2/euce1: Unified Platform
      → 반드시 region + product_id로 조인해야 함
*/

WITH source AS (
    SELECT * FROM {{ source('tesla', 'products') }}
),

renamed AS (
    SELECT
        CAST(id AS STRING) AS product_id,
        region,
        name AS product_name,
        product_type,
        model_type,
        CAST(recurring_billing AS BOOLEAN) AS is_recurring,
        CAST(qa_service_enabled AS BOOLEAN) AS qa_service_enabled,
        code AS product_code,
        TIMESTAMP(created_at) AS created_at,
        TIMESTAMP(updated_at) AS updated_at
    FROM source
)

SELECT * FROM renamed