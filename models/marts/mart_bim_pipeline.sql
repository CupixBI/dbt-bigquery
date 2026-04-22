with source as (
    select * from {{ ref('fct_capture_processing_enriched') }}
),

final as (
    select
        region_capture_id,
        region_record_id,
        region_level_id,
        project_id                                                              as region_facility_id,
        project_name,
        team_name,
        region,
        tenant,
        uploading_finished_at_kst,
        bim_floorplan_created_at,

        -- Phase 1: 모수 파악
        bim_dwg_count > 0                                                       as is_bim_level,
        bim_dwg_count,
        floorplan_count,

        -- Phase 2 퍼널 플래그 (분기 순서대로, 분모 상속 구조)
        is_captured_after_bim_created,                                          -- Bridge 2: 분기1 분모 자격
        refinement_floorplan_type = 'bim'                                       as is_dwg_selected,        -- 분기1
        refinement_result_exists,                                               --            분기2
        cqa_refinement_selected_type = 'refinement_with_prior_map'             as is_refinement_adopted,  -- 분기3

        -- Phase 3: 원인 분리
        preview_quality,

        -- 슬라이싱용 컨텍스트
        capture_type,
        camera_model_name,
        editor_level,
        refinement_floorplan_type,
        cqa_refinement_selected_type

    from source
)

select * from final
