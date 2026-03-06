/*
    fct_team_summary.sql
    
    목적: 팀(=회사) 단위 최종 요약 테이블
      - 매출 정보 (int_team_revenue)
      - 활성 워크스페이스 수, 퍼실리티 수 (int_facility_details)
      - 캡처 주기, 에러율 (int_capture_details)
    
    그레인: 1행 = 1팀
    
    워크스페이스 수: workspace_lock_state = 'active'
    퍼실리티 수: workspace_lock_state = 'active' AND facility_cycle_state = 'created'
    캡처 주기: 팀별 연속 캡처 간 평균 일수 (캡처 1개면 NULL)
    에러율: (error_code IS NOT NULL OR reconstruction_error_code IS NOT NULL) / 전체 캡처 수
*/

WITH team_revenue AS (
    SELECT * FROM {{ ref('int_team_revenue') }}
),

facility_detail AS (
    SELECT * FROM {{ ref('int_facility_details') }}
),

capture_detail AS (
    SELECT * FROM {{ ref('int_capture_details') }}
),

users_with_teams AS (
    SELECT * FROM {{ ref('int_users_with_teams') }}
),

-- facility_detail에서 팀별 집계
team_facility_stats AS (
    SELECT
        team_id,
        region,

        COUNT(DISTINCT CASE
            WHEN workspace_lock_state = 'active'
            THEN workspace_id
        END) AS active_workspaces,

        COUNT(DISTINCT CASE
            WHEN workspace_lock_state = 'active'
             AND facility_cycle_state = 'created'
            THEN facility_id
        END) AS active_facilities,

        -- 실제 사용 면적 (SqFt 통일, UNKNOWN 제외)
        SUM(CASE
            WHEN workspace_lock_state = 'active'
             AND facility_cycle_state = 'created'
             AND facility_size_unit = 'SQFT' THEN facility_size
            WHEN workspace_lock_state = 'active'
             AND facility_cycle_state = 'created'
             AND facility_size_unit = 'SQM' THEN facility_size * 10.7639
            ELSE 0
        END) AS used_area_sqft,

        -- 캡처된 면적 (단위 없이 원본 숫자 합산)
        SUM(CASE
            WHEN workspace_lock_state = 'active'
             AND facility_cycle_state = 'created'
            THEN COALESCE(captured_size, 0)
            ELSE 0
        END) AS total_captured_size

    FROM facility_detail
    GROUP BY 1, 2
),

-- capture_detail에서 팀별 집계
-- 1) 캡처별 이전 캡처와의 간격 계산
capture_with_interval AS (
    SELECT
        region_team_id,
        created_at,
        error_code,
        reconstruction_error_code,
        DATE_DIFF(
            DATE(created_at),
            DATE(LAG(created_at) OVER (
                PARTITION BY region_team_id
                ORDER BY created_at
            )),
            DAY
        ) AS days_since_prev_capture
    FROM capture_detail
),

-- 2) 팀별 캡처 주기(평균) + 에러율
team_capture_stats AS (
    SELECT
        region_team_id,

        -- 캡처 수
        COUNT(*) AS total_captures,

        -- 캡처 주기 (일): 캡처 1개면 NULL
        CASE
            WHEN COUNT(*) <= 1 THEN NULL
            ELSE AVG(days_since_prev_capture)
        END AS avg_capture_interval_days,

        -- 에러 캡처 수
        COUNTIF(
            error_code IS NOT NULL
            OR reconstruction_error_code IS NOT NULL
        ) AS error_captures,

        -- 에러율
        SAFE_DIVIDE(
            COUNTIF(
                error_code IS NOT NULL
                OR reconstruction_error_code IS NOT NULL
            ),
            COUNT(*)
        ) AS capture_error_rate

    FROM capture_with_interval
    GROUP BY 1
),

-- users_with_teams에서 팀별 활성 유저 수
team_user_stats AS (
    SELECT
        team_id,
        team_region AS region,
        COUNT(DISTINCT user_id) AS active_users
    FROM users_with_teams
    WHERE team_lock_state = 'active'
      AND user_state = 'active'
      AND user_cycle_state = 'created'
    GROUP BY 1, 2
),

final AS (
    SELECT
        tr.*,

        -- Facility 현황
        COALESCE(fs.active_workspaces, 0) AS active_workspaces,
        COALESCE(fs.active_facilities, 0) AS active_facilities,

        -- 면적 (SqFt) — contracted_area_sqft는 tr.*에 포함
        COALESCE(fs.used_area_sqft, 0) AS used_area_sqft,
        COALESCE(fs.total_captured_size, 0) AS total_captured_size,
        SAFE_DIVIDE(
            COALESCE(fs.used_area_sqft, 0),
            NULLIF(tr.contracted_area_sqft, 0)
        ) AS area_utilization_rate,

        -- 유저 현황
        COALESCE(us.active_users, 0) AS active_users,

        -- Capture 현황
        COALESCE(cs.total_captures, 0) AS total_captures,
        cs.avg_capture_interval_days,
        COALESCE(cs.error_captures, 0) AS error_captures,
        COALESCE(cs.capture_error_rate, 0) AS capture_error_rate,

        -- 메타
        CURRENT_TIMESTAMP() AS updated_at

    FROM team_revenue tr

    LEFT JOIN team_facility_stats fs
        ON tr.team_id = fs.team_id
        AND tr.region = fs.region

    LEFT JOIN team_user_stats us
        ON tr.team_id = us.team_id
        AND tr.region = us.region

    LEFT JOIN team_capture_stats cs
        ON tr.region_team_id = cs.region_team_id
)

SELECT * FROM final