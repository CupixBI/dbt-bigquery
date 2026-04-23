WITH capture_traces AS (
    SELECT * FROM {{ ref('stg_cupixworks__capture_traces') }}
),

final AS (
    SELECT *
    FROM capture_traces
    WHERE (class_name = 'Capture' OR class = 'Capture')
      AND lower(stage) IN (
          'editing_waiting',
          'editing_in_review',
          'editing_holding',
          'editing_done',
          'editing_editing',
          'editing_waiting_for_review',
          'editing_ready',
          'editing_escalated',
          'preview_finished'
      )
)

SELECT * FROM final
