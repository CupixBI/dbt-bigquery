WITH element_records AS (
    SELECT * FROM {{ ref('stg_cupixworks__element_records') }}
),

bims AS (
    SELECT
        region_bim_id,
        last_bim_revision_id
    FROM {{ ref('stg_tesla__bims') }}
),

final AS (
    SELECT er.*
    FROM element_records er
    INNER JOIN bims b
        ON er.region_bim_id = b.region_bim_id
    WHERE er.bim_revision_id = b.last_bim_revision_id
       OR (er.bim_revision_id IS NULL AND b.last_bim_revision_id IS NULL)
)

SELECT * FROM final
