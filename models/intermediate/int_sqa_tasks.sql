WITH capture_traces AS (
    SELECT * FROM {{ ref('stg_cupixworks__capture_traces') }}
),

final AS (
    SELECT *
    FROM capture_traces
    WHERE (class_name = 'Editing' OR class = 'Editing')
      AND lower(stage) LIKE '%editing_%'
)

SELECT * FROM final
