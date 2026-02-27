/*
    stg_line_items.sql
    
    SF 정보 원칙:
      - sf_opportunity_id (조인 키)만 유지
      - 나머지 SF 정보(contract_type, siteinsights 등)는
        stg_sf_opportunities에서 가져옴 (Single Source of Truth)
    
    제품 정보:
      - stg_products와 region + product_id로 조인
      - 같은 product_id가 region별로 다른 제품이므로 region 조인 필수
*/

WITH source AS (
    SELECT * FROM {{ source('tesla', 'line_items') }}
),

products AS (
    SELECT * FROM {{ ref('stg_tesla__products') }}
),

renamed AS (
    SELECT
        CAST(li._id AS STRING) AS line_item_id,
        CAST(li.quote_id AS STRING) AS quote_id,
        CAST(li.product_id AS STRING) AS product_id,
        p.product_name,
        p.product_type,
        li.region,
        li.name AS line_item_name,
        TIMESTAMP(li.created_at) AS created_at,
        TIMESTAMP(li.disabled_at) AS disabled_at,

        -- SF 조인 키만
        li.sf_opportunity_id

    FROM source li

    LEFT JOIN products p
        ON CAST(li.product_id AS STRING) = p.product_id
        AND li.region = p.region
)

SELECT * FROM renamed