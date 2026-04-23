WITH capture_traces AS (
    SELECT * FROM {{ ref('stg_cupixworks__capture_traces') }}
),

final AS (
    SELECT *
    FROM capture_traces
    WHERE class_name = 'Capture'
       OR class = 'Capture'
)

SELECT * FROM final
