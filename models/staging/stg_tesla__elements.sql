WITH source AS (
    SELECT * FROM {{ source('tesla', 'elements') }}
),

renamed AS (
    SELECT
        region,
        CAST(_id AS STRING)                         AS element_id,
        CAST(id AS STRING)                          AS id,
        tenant,
        CAST(facility_id AS STRING)                 AS facility_id,
        CAST(team_id AS STRING)                     AS team_id,
        CAST(workspace_id AS STRING)                AS workspace_id,
        CAST(level_id AS STRING)                    AS level_id,
        CAST(user_id AS STRING)                     AS user_id,
        CAST(category_id AS STRING)                 AS category_id,
        CAST(bim_id AS STRING)                      AS bim_id,
        CAST(bim_revision_id AS STRING)             AS bim_revision_id,
        CAST(bim_object_id AS STRING)               AS bim_object_id,
        CAST(bim_model_id AS STRING)                AS bim_model_id,
        CAST(cycle_state_updated_by_id AS STRING)   AS cycle_state_updated_by_id,
        bim_element_id,
        bim_external_id,
        ifc_guid,
        guid,
        uuid,
        key,
        name,
        platform,
        state,
        cycle_state,
        source_file,
        length_key_name,
        weight_key_name,
        area_key_name,
        volume_key_name,
        length,
        weight,
        area,
        volume,
        cost,
        activity_ids,
        customs,
        meta,
        cycle_state_updated_at,
        state_updated_at,
        created_at,
        updated_at,
    FROM source
),

final AS (
    SELECT
        region,
        element_id,
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
            '-',
            element_id,
            '-',
            tenant
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
            '-',
            facility_id,
            '-',
            tenant
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
            '-',
            team_id,
            '-',
            tenant
        ) AS region_team_id,
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
            '-',
            workspace_id,
            '-',
            tenant
        ) AS region_workspace_id,
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
            '-',
            level_id,
            '-',
            tenant
        ) AS region_level_id,
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
            '-',
            user_id,
            '-',
            tenant
        ) AS region_user_id,
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
            '-',
            bim_id,
            '-',
            tenant
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
            '-', facility_id,
            '-', bim_id,
            '-', bim_external_id,
            '-', bim_revision_id,
            '-', cycle_state,
            '-', tenant
        ) AS region_bim_element_revision_id,
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
        id,
        tenant,
        facility_id,
        team_id,
        workspace_id,
        level_id,
        user_id,
        category_id,
        bim_id,
        bim_revision_id,
        bim_object_id,
        bim_model_id,
        cycle_state_updated_by_id,
        bim_element_id,
        bim_external_id,
        ifc_guid,
        guid,
        uuid,
        key,
        name,
        platform,
        state,
        cycle_state,
        source_file,
        length_key_name,
        weight_key_name,
        area_key_name,
        volume_key_name,
        length,
        weight,
        area,
        volume,
        cost,
        activity_ids,
        customs,
        meta,
        cycle_state_updated_at,
        state_updated_at,
        created_at,
        updated_at,
    FROM renamed
)

SELECT * FROM final
WHERE (region_facility_id IS NOT NULL
  OR region_bim_id IS NOT NULL
  OR region_bim_external_id IS NOT NULL)
  AND cycle_state = 'created'
