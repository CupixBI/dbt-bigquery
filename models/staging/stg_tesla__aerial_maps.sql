with

source as (
    select * from {{ source('tesla', 'aerial_maps') }}
),

-- 1단계: 구조 정리 (타입 변환 및 컬럼명 변경)
renamed as (
    select
        region,
        cast(_id as string) as aerial_map_id,
        key,
        cast(facility_id as string) as facility_id,
        cast(workspace_id as string) as workspace_id,
        cast(team_id as string) as team_id,
        cast(user_id as string) as user_id,
        cast(pix4d_project_id as string) as pix4d_project_id,

        name as aerial_map_name,
        tenant,

        -- states
        state,
        cycle_state,
        dsm_state,
        dsm_tile_state,
        pointcloud_state,
        potree_state,
        mesh_state,
        thumbnail_state,
        orthomosaic_state,
        orthomosaic_tile_state,
        report_state,

        -- error
        error_reason,
        error_code,

        -- dsm_georeference
        dsm_georeference.srs                           as dsm_srs,
        dsm_georeference.coordinates.latitude           as dsm_latitude,
        dsm_georeference.coordinates.longitude          as dsm_longitude,

        -- orthomosaic_georeference
        orthomosaic_georeference.srs                    as ortho_srs,
        orthomosaic_georeference.coordinates.latitude   as ortho_latitude,
        orthomosaic_georeference.coordinates.longitude  as ortho_longitude,

        -- sys: file info
        sys.dsm_file_extension,
        sys.dsm_file_size,
        sys.orthomosaic_file_extension,
        sys.orthomosaic_file_size,
        sys.pointcloud_file_extension,
        sys.pointcloud_file_size,
        sys.mesh_file_extension,
        sys.mesh_file_size,

        -- sys: resolution
        sys.dsm_resolution.height                       as dsm_resolution_height,
        sys.dsm_resolution.width                        as dsm_resolution_width,
        sys.orthomosaic_resolution.height               as ortho_resolution_height,
        sys.orthomosaic_resolution.width                as ortho_resolution_width,
        sys.image_resolution.height                     as image_resolution_height,
        sys.image_resolution.width                      as image_resolution_width,

        -- sys: dsm elevation
        sys.dsm_elevation.max                           as dsm_elevation_max,
        sys.dsm_elevation.min                           as dsm_elevation_min,

        -- sys: tile pixel
        sys.dsm_tile_pixel,
        sys.orthomosaic_tile_pixel,

        -- sys: image & camera
        sys.image_count,
        sys.image_extension,
        sys.camera_model,
        sys.camera_maker,

        -- sys: point cloud
        sys.point_density,
        sys.point_count,

        -- sys: rms error
        sys.rms_error.xyz                               as rms_error_xyz,
        sys.rms_error.xy                                as rms_error_xy,
        sys.rms_error.z                                 as rms_error_z,
        sys.rms_error.y                                 as rms_error_y,
        sys.rms_error.x                                 as rms_error_x,

        -- sys: processing timestamps (INT64 → TIMESTAMP)
        timestamp_millis(sys.preprocess_begin_timestamp)    as preprocess_begin_at,
        timestamp_millis(sys.preprocess_end_timestamp)      as preprocess_end_at,
        timestamp_millis(sys.process_begin_timestamp)       as process_begin_at,
        timestamp_millis(sys.process_end_timestamp)         as process_end_at,
        timestamp_millis(sys.postprocess_begin_timestamp)   as postprocess_begin_at,
        timestamp_millis(sys.postprocess_end_timestamp)     as postprocess_end_at,

        -- processing_option
        processing_option.resolution                    as processing_resolution,
        processing_option.method                        as processing_method,
        'orthomosaic' in unnest(processing_option.outputs)  as is_orthomosaic,
        'pointcloud' in unnest(processing_option.outputs)   as is_pointcloud,
        'mesh' in unnest(processing_option.outputs)         as is_mesh,
        'dsm' in unnest(processing_option.outputs)          as is_dsm,

        -- timestamps
        timestamp(captured_at)                                              as captured_at,
        timestamp(published_at)                                             as published_at,
        timestamp(created_at)                                               as created_at,
        timestamp(updated_at)                                               as updated_at,
        timestamp(uploaded_at)              as uploaded_at,
        timestamp(cleaned_at)              as cleaned_at,
        timestamp(purged_at)               as purged_at,
        timestamp(state_updated_at)        as state_updated_at,
        timestamp(cycle_state_updated_at)  as cycle_state_updated_at

    from source
),

-- 2단계: Null 처리 및 비즈니스 로직
final as (
    select
        -- [기본 컬럼]
        region,
        aerial_map_id,
        concat(
            case region
                when 'uswe2' then 'US'
                when 'apse2' then 'AU'
                when 'euce1' then 'EU'
                when 'apne1' then 'JP'
                when 'apse1' then 'SG'
                when 'cace1' then 'CA'
                else 'Unknown'
            end,
            '-',
            aerial_map_id
        ) as region_aerial_map_id,

        key,
        aerial_map_name,
        tenant,

        captured_at,
        created_at,

        -- [FK 및 참조값]
        facility_id,
        concat(
            case region
                when 'uswe2' then 'US'
                when 'apse2' then 'AU'
                when 'euce1' then 'EU'
                when 'apne1' then 'JP'
                when 'apse1' then 'SG'
                when 'cace1' then 'CA'
                else 'Unknown'
            end,
            '-',
            facility_id
        ) as region_facility_id,

        workspace_id,
        concat(
            case region
                when 'uswe2' then 'US'
                when 'apse2' then 'AU'
                when 'euce1' then 'EU'
                when 'apne1' then 'JP'
                when 'apse1' then 'SG'
                when 'cace1' then 'CA'
                else 'Unknown'
            end,
            '-',
            workspace_id
        ) as region_workspace_id,

        team_id,
        concat(
            case region
                when 'uswe2' then 'US'
                when 'apse2' then 'AU'
                when 'euce1' then 'EU'
                when 'apne1' then 'JP'
                when 'apse1' then 'SG'
                when 'cace1' then 'CA'
                else 'Unknown'
            end,
            '-',
            team_id
        ) as region_team_id,

        user_id,
        concat(
            case region
                when 'uswe2' then 'US'
                when 'apse2' then 'AU'
                when 'euce1' then 'EU'
                when 'apne1' then 'JP'
                when 'apse1' then 'SG'
                when 'cace1' then 'CA'
                else 'Unknown'
            end,
            '-',
            user_id
        ) as region_user_id,

        pix4d_project_id,

        -- [상태값]
        coalesce(state, 'Unknown')                  as state,
        coalesce(cycle_state, 'Unknown')            as cycle_state,
        coalesce(dsm_state, 'Unknown')              as dsm_state,
        coalesce(dsm_tile_state, 'Unknown')         as dsm_tile_state,
        coalesce(pointcloud_state, 'Unknown')       as pointcloud_state,
        coalesce(potree_state, 'Unknown')           as potree_state,
        coalesce(mesh_state, 'Unknown')             as mesh_state,
        coalesce(thumbnail_state, 'Unknown')        as thumbnail_state,
        coalesce(orthomosaic_state, 'Unknown')      as orthomosaic_state,
        coalesce(orthomosaic_tile_state, 'Unknown') as orthomosaic_tile_state,
        coalesce(report_state, 'Unknown')           as report_state,

        -- [에러]
        error_reason,
        error_code,

        -- [좌표]
        dsm_srs,
        dsm_latitude,
        dsm_longitude,
        ortho_srs,
        ortho_latitude,
        ortho_longitude,

        -- [파일 정보]
        dsm_file_extension,
        coalesce(dsm_file_size, -1)             as dsm_file_size,
        orthomosaic_file_extension,
        coalesce(orthomosaic_file_size, -1)     as orthomosaic_file_size,
        pointcloud_file_extension,
        coalesce(pointcloud_file_size, -1)      as pointcloud_file_size,
        mesh_file_extension,
        coalesce(mesh_file_size, -1)            as mesh_file_size,

        -- [해상도]
        dsm_resolution_height,
        dsm_resolution_width,
        ortho_resolution_height,
        ortho_resolution_width,
        image_resolution_height,
        image_resolution_width,

        -- [DSM 고도]
        dsm_elevation_max,
        dsm_elevation_min,

        -- [타일]
        dsm_tile_pixel,
        orthomosaic_tile_pixel,

        -- [이미지 & 카메라]
        coalesce(image_count, -1)               as image_count,
        image_extension,
        coalesce(camera_model, 'Unknown')       as camera_model,
        coalesce(camera_maker, 'Unknown')       as camera_maker,

        -- [포인트클라우드]
        point_density,
        point_count,

        -- [RMS 오차]
        rms_error_xyz,
        rms_error_xy,
        rms_error_z,
        rms_error_y,
        rms_error_x,

        -- [프로세싱 타임스탬프]
        preprocess_begin_at,
        preprocess_end_at,
        process_begin_at,
        process_end_at,
        postprocess_begin_at,
        postprocess_end_at,

        -- [프로세싱 옵션]
        coalesce(processing_resolution, 'Unknown')  as processing_resolution,
        coalesce(processing_method, 'Unknown')      as processing_method,
        is_orthomosaic,
        is_pointcloud,
        is_mesh,
        is_dsm,

        -- [타임스탬프]
        published_at,
        updated_at,
        uploaded_at,
        cleaned_at,
        purged_at,
        state_updated_at,
        cycle_state_updated_at

    from renamed
)

select * from final