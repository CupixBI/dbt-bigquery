/*
    stg_applied_products.sql
    
    목적: 제품 활성화 상태 (ON/OFF)
    
    line_item + product가 있어도 applied_product가 active가 아니면
    실제 유저가 사용할 수 없음
*/

WITH source AS (
    SELECT * FROM {{ source('tesla', 'applied_products') }}
),

renamed AS (
    SELECT
        CAST(_id AS STRING) AS applied_product_id,
        region,
        CAST(quote_id AS STRING) AS quote_id,
        CAST(product_id AS STRING) AS product_id,
        CAST(billable_id AS STRING) AS billable_id,
        billable_type,
        state AS applied_state,
        CAST(user_id AS STRING) AS user_id,
        TIMESTAMP(created_at) AS created_at,
        TIMESTAMP(updated_at) AS updated_at
    FROM source
)

SELECT * FROM renamed