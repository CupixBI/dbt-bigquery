
WITH dates AS (
  SELECT DISTINCT DATE(uploading_finished_at) as event_date
  FROM {{ ref('fct_capture_processing_enriched') }}
  WHERE uploading_finished_at IS NOT NULL
  
  UNION DISTINCT
  
  SELECT DISTINCT DATE(edit_finished_at) as event_date
  FROM {{ ref('fct_capture_processing_enriched') }}
  WHERE edit_finished_at IS NOT NULL
),

inflow AS (
  SELECT 
    DATE(uploading_finished_at) as event_date,
    COUNT(*) as inflow_count
  FROM {{ ref('fct_capture_processing_enriched') }}
  WHERE uploading_finished_at IS NOT NULL
  GROUP BY event_date
),

outflow AS (
  SELECT 
    DATE(edit_finished_at) as event_date,
    COUNT(*) as outflow_count
  FROM {{ ref('fct_capture_processing_enriched') }}
  WHERE edit_finished_at IS NOT NULL
  GROUP BY event_date
),

daily_flow AS (
  SELECT 
    d.event_date,
    COALESCE(i.inflow_count, 0) as inflow_count,
    COALESCE(o.outflow_count, 0) as outflow_count,
    COALESCE(i.inflow_count, 0) - COALESCE(o.outflow_count, 0) as net_flow
  FROM dates d
  LEFT JOIN inflow i ON d.event_date = i.event_date
  LEFT JOIN outflow o ON d.event_date = o.event_date
)

SELECT 
  event_date,
  inflow_count,
  outflow_count,
  net_flow,
  SUM(net_flow) OVER (ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_difference,
  SUM(inflow_count) OVER (ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_inflow,
  SUM(outflow_count) OVER (ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_outflow,
  CASE WHEN net_flow > 0 THEN net_flow ELSE 0 END as positive_flow,
  CASE WHEN net_flow < 0 THEN net_flow ELSE 0 END as negative_flow
FROM daily_flow
ORDER BY event_date