/*
    stg_tesla__groups.sql
    
    목적: 팀 내 그룹 마스터 테이블
    주 용도: assigned_customer_success_managers 그룹 → CSM 배정 조회
*/

WITH source AS (
    SELECT * FROM {{ source('tesla', 'groups') }}
),

renamed AS (
    SELECT
        CAST(_id AS STRING) AS group_id,
        region,
        CAST(team_id AS STRING) AS team_id,
        group_type_code,
        name AS group_name,
        users_count,
        cycle_state,
        TIMESTAMP(created_at) AS created_at,
        TIMESTAMP(updated_at) AS updated_at,
        tenant
    FROM source
)

SELECT * FROM renamed