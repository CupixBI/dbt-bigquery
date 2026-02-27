/*
    int_quotes_classified.sql
    
    목적: billable 단위당 유효 quote 1개만 선별하여 영구 보존 (incremental)
    
    중복 제거 근거:
      - Team 라이센스: 한 팀에 활성 quote는 항상 1개 (Builder/Unified 공존 불가)
      - Workspace 라이센스: 한 workspace에 활성 quote는 항상 1개
    
    Incremental 전략:
      - 매 빌드 시 ROW_NUMBER() = 1 계산 → 기존 테이블에 없는 신규 유효 quote만 INSERT
      - 한번 유효로 판정된 quote는 영구 보존 (갱신으로 rank가 밀려도 삭제되지 않음)
      - is_currently_billing은 이 모델에서 계산하지 않음 (다운스트림에서 billing 기간 기준으로 계산)
*/

{{
    config(
        materialized='incremental',
        unique_key=['region', 'quote_id']
    )
}}

WITH quotes AS (
    SELECT * FROM {{ ref('stg_tesla__quotes') }}
    WHERE quote_type = 'fixed_price'
      AND state = 'applied'
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY region, billable_type, billable_id
            ORDER BY created_at DESC
        ) AS quote_rank
    FROM quotes
),

current_valid AS (
    SELECT
        region,
        quote_id,
        region_quote_id,
        billable_id,
        region_billable_id,
        billable_type,
        billing_started_at,
        billing_expires_at,
        contract_months,
        created_at,
        quote_name,
        is_pilot,
        quote_type,
        state,
        created_by_user_id,
        region_created_by_user_id
    FROM ranked
    WHERE quote_rank = 1
)

SELECT * FROM current_valid

{% if is_incremental() %}
    -- 이미 존재하는 quote는 다시 INSERT하지 않음
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} existing
        WHERE existing.region = current_valid.region
          AND existing.quote_id = current_valid.quote_id
    )
{% endif %}