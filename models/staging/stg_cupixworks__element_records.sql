WITH source AS (
    SELECT * FROM {{ source('cupixworks', 'element_records') }}
),

renamed AS (
    SELECT
        region,
        CAST(id AS STRING)                                          AS element_record_id,
        TRIM(REPLACE(tenant, '"', ''))                              AS tenant,
        timestamp,
        _updated_at,

        -- element identification
        JSON_VALUE(raw_json, '$.bim_external_id')                   AS bim_external_id,
        JSON_VALUE(raw_json, '$.guid')                              AS guid,
        JSON_VALUE(raw_json, '$.cycle_state')                       AS cycle_state,

        -- element snapshot
        CAST(JSON_VALUE(raw_json, '$.element.id') AS STRING)        AS element_id,
        JSON_VALUE(raw_json, '$.element.bim_element_id')            AS bim_element_id,
        JSON_VALUE(raw_json, '$.element.bim_revision_id')           AS bim_revision_id,
        CAST(JSON_VALUE(raw_json, '$.element.bim_model_id') AS STRING)  AS bim_model_id,
        CAST(JSON_VALUE(raw_json, '$.element.bim_object_id') AS STRING) AS bim_object_id,
        JSON_VALUE(raw_json, '$.element.cycle_state')               AS element_cycle_state,
        JSON_VALUE(raw_json, '$.element.name')                      AS element_name,
        JSON_VALUE(raw_json, '$.element.platform')                  AS platform,

        -- bim
        CAST(JSON_VALUE(raw_json, '$.bim.id') AS STRING)            AS bim_id,
        JSON_VALUE(raw_json, '$.bim.name')                          AS bim_name,

        -- facility, level, workspace, team
        CAST(JSON_VALUE(raw_json, '$.facility.id') AS STRING)       AS facility_id,
        JSON_VALUE(raw_json, '$.facility.key')                      AS facility_key,
        CAST(JSON_VALUE(raw_json, '$.level.id') AS STRING)          AS level_id,
        JSON_VALUE(raw_json, '$.level.name')                        AS level_name,
        CAST(JSON_VALUE(raw_json, '$.level.elevation') AS FLOAT64)  AS level_elevation,
        CAST(JSON_VALUE(raw_json, '$.workspace.id') AS STRING)      AS workspace_id,
        CAST(JSON_VALUE(raw_json, '$.team.id') AS STRING)           AS team_id,

        -- task
        CAST(JSON_VALUE(raw_json, '$.task.id') AS STRING)           AS task_id,
        JSON_VALUE(raw_json, '$.task.cycle_state')                  AS task_cycle_state,

        -- category
        CAST(JSON_VALUE(raw_json, '$.category.id') AS STRING)       AS category_id,
        JSON_VALUE(raw_json, '$.category.name')                     AS category_name,
        JSON_VALUE(raw_json, '$.category.category_type')            AS category_type,
        CAST(JSON_VALUE(raw_json, '$.main_category.id') AS STRING)  AS main_category_id,
        JSON_VALUE(raw_json, '$.main_category.name')                AS main_category_name,

        -- phase
        CAST(JSON_VALUE(raw_json, '$.phase.id') AS STRING)          AS phase_id,
        JSON_VALUE(raw_json, '$.phase.name')                        AS phase_name,

        -- workarea
        CAST(JSON_VALUE(raw_json, '$.workarea.id') AS STRING)       AS workarea_id,
        JSON_VALUE(raw_json, '$.workarea.name')                     AS workarea_name,

        -- metrics
        CAST(JSON_VALUE(raw_json, '$.area') AS FLOAT64)             AS area,
        CAST(JSON_VALUE(raw_json, '$.volume') AS FLOAT64)           AS volume,
        CAST(JSON_VALUE(raw_json, '$.weight') AS FLOAT64)           AS weight,
        CAST(JSON_VALUE(raw_json, '$.length') AS FLOAT64)           AS length,
        CAST(JSON_VALUE(raw_json, '$.cost') AS FLOAT64)             AS cost,

        -- dates
        TIMESTAMP(JSON_VALUE(raw_json, '$.created_at."$date"'))     AS created_at,
        TIMESTAMP(JSON_VALUE(raw_json, '$.updated_at."$date"'))     AS updated_at,
        TIMESTAMP(JSON_VALUE(raw_json, '$.incompleted_at."$date"')) AS incompleted_at,
        TIMESTAMP(JSON_VALUE(raw_json, '$.start_at."$date"'))       AS start_at,
        TIMESTAMP(JSON_VALUE(raw_json, '$.end_at."$date"'))         AS end_at,
        CAST(JSON_VALUE(raw_json, '$.start_week') AS INT64)         AS start_week,
        CAST(JSON_VALUE(raw_json, '$.end_week') AS INT64)           AS end_week,
        CAST(JSON_VALUE(raw_json, '$.incompleted_week') AS INT64)   AS incompleted_week,

    FROM source
),

final AS (
    SELECT
        region,
        element_record_id,
        CONCAT(
            CASE region
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-', element_record_id,
            '-', tenant
        ) AS region_element_record_id,
        CONCAT(
            CASE region
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-', element_id,
            '-', tenant
        ) AS region_element_id,
        CONCAT(
            CASE region
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-', facility_id,
            '-', tenant
        ) AS region_facility_id,
        CONCAT(
            CASE region
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-', bim_id,
            '-', tenant
        ) AS region_bim_id,
        CONCAT(
            CASE region
                WHEN 'uswe2' THEN 'US'
                WHEN 'apse2' THEN 'AU'
                WHEN 'euce1' THEN 'EU'
                WHEN 'apne1' THEN 'JP'
                WHEN 'apse1' THEN 'SG'
                WHEN 'cace1' THEN 'CA'
                ELSE 'Unknown'
            END,
            '-', bim_external_id,
            '-', tenant
        ) AS region_bim_external_id,
        tenant,
        timestamp,
        _updated_at,
        bim_external_id,
        guid,
        cycle_state,
        element_id,
        bim_element_id,
        bim_revision_id,
        bim_model_id,
        bim_object_id,
        element_cycle_state,
        element_name,
        platform,
        bim_id,
        bim_name,
        facility_id,
        facility_key,
        level_id,
        level_name,
        level_elevation,
        workspace_id,
        team_id,
        task_id,
        task_cycle_state,
        category_id,
        category_name,
        category_type,
        main_category_id,
        main_category_name,
        phase_id,
        phase_name,
        workarea_id,
        workarea_name,
        area,
        volume,
        weight,
        length,
        cost,
        created_at,
        updated_at,
        incompleted_at,
        start_at,
        end_at,
        start_week,
        end_week,
        incompleted_week,
    FROM renamed
)

SELECT * FROM final
WHERE (region_facility_id IS NOT NULL
  OR region_bim_id IS NOT NULL
  OR region_bim_external_id IS NOT NULL)
  AND cycle_state = 'created'
  AND element_cycle_state = 'created'
