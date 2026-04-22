with source as (
    select * from {{ ref('fct_capture_processing_enriched') }}
    where bim_dwg_count > 0
),

final as (
    select
        region_level_id,
        project_id                                      as region_facility_id,
        project_name,
        team_name,
        region,
        tenant,

        -- 분기1 분모: DWG 생성일 이후 촬영된 capture 수
        countif(is_captured_after_bim_created = true)
                                                        as after_bim_capture_count,

        -- 분기1: DWG 선택률
        countif(
            is_captured_after_bim_created = true
            and refinement_floorplan_type = 'bim'
        )                                               as dwg_selected_count,
        safe_divide(
            countif(
                is_captured_after_bim_created = true
                and refinement_floorplan_type = 'bim'
            ),
            countif(is_captured_after_bim_created = true)
        )                                               as dwg_selection_rate,

        -- 분기2: Refinement 성공률 (분모 상속: dwg_selected)
        countif(
            is_captured_after_bim_created = true
            and refinement_floorplan_type = 'bim'
            and refinement_result_exists = true
        )                                               as refinement_success_count,
        safe_divide(
            countif(
                is_captured_after_bim_created = true
                and refinement_floorplan_type = 'bim'
                and refinement_result_exists = true
            ),
            countif(
                is_captured_after_bim_created = true
                and refinement_floorplan_type = 'bim'
            )
        )                                               as refinement_success_rate,

        -- 분기3: Refinement 채택률 (분모 상속: refinement_success)
        countif(
            is_captured_after_bim_created = true
            and refinement_floorplan_type = 'bim'
            and refinement_result_exists = true
            and cqa_refinement_selected_type = 'refinement_with_prior_map'
        )                                               as refinement_adopted_count,
        safe_divide(
            countif(
                is_captured_after_bim_created = true
                and refinement_floorplan_type = 'bim'
                and refinement_result_exists = true
                and cqa_refinement_selected_type = 'refinement_with_prior_map'
            ),
            countif(
                is_captured_after_bim_created = true
                and refinement_floorplan_type = 'bim'
                and refinement_result_exists = true
            )
        )                                               as refinement_adoption_rate

    from source
    group by 1, 2, 3, 4, 5, 6
)

select * from final
