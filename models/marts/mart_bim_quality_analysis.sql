with source as (
    select * from {{ ref('mart_bim_pipeline') }}
    where is_bim_level = true
),

final as (
    select
        region_record_id,
        region_level_id,
        region_facility_id,
        project_name,
        team_name,
        region,
        tenant,
        any_value(preview_quality)                          as preview_quality,

        -- 분기1 분모: DWG 생성일 이후 촬영된 capture 수
        countif(is_captured_after_bim_created = true)       as after_bim_capture_count,

        -- 분기1: DWG 선택 (분모 상속 없음, Bridge 2 조건만)
        countif(
            is_captured_after_bim_created = true
            and is_dwg_selected = true
        )                                                   as dwg_selected_count,

        -- 분기2: Refinement 성공 (분모 상속: dwg_selected)
        countif(
            is_captured_after_bim_created = true
            and is_dwg_selected = true
            and refinement_result_exists = true
        )                                                   as refinement_success_count,

        -- 분기3: Refinement 채택 (분모 상속: refinement_success)
        countif(
            is_captured_after_bim_created = true
            and is_dwg_selected = true
            and refinement_result_exists = true
            and is_refinement_adopted = true
        )                                                   as refinement_adopted_count

    from source
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from final
