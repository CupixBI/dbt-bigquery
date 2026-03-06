/*
    stg_tesla__grouped_users.sql
    
    목적: 그룹에 배정된 유저 매핑 테이블
*/

WITH source AS (
    SELECT * FROM {{ source('tesla', 'grouped_users') }}
),

renamed AS (
    SELECT
        CAST(_id AS STRING) AS grouped_user_id,
        region,
        CAST(group_id AS STRING) AS group_id,
        CAST(user_id AS STRING) AS user_id,
        TIMESTAMP(created_at) AS created_at,
        TIMESTAMP(updated_at) AS updated_at
    FROM source
)

SELECT * FROM renamed