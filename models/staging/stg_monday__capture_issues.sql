WITH source AS(
    select * FROM {{ source('monday', 'capture_issues_native') }}
),

filtered AS (
    SELECT *
    FROM source
    -- [첫 번째 행 삭제 로직]
    -- 만약 첫 행이 헤더(컬럼명)라서 지우는 것이라면,
    -- 특정 컬럼(예: capture_name)의 값이 컬럼명 자체와 같은지 확인하여 제외합니다.
    WHERE name != 'Name'
    AND capture_id IS NOT NULL
),

renamed AS (
    SELECT
        name AS issue_name,
        status,
        server as region,
        capture_id,
        CONCAT(
            server,
            '-',
            capture_id
        ) AS region_capture_id,
        record_id,
        TIMESTAMP(created_at) as created_at,
        task_id,
        url,
        assignee,
        reporter,
        message,
        follow_up,
        issue_code
    FROM filtered
)

SELECT * FROM renamed