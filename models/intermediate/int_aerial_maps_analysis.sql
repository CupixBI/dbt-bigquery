with

aerial_maps as (
    select * from {{ ref('stg_tesla__aerial_maps') }}
),

facility_details as (
    select * from {{ ref('int_facility_details') }}
),

-- 1단계: facility 정보 JOIN
joined as (
    select
        am.*,
        fd.facility_name,
        -- fd.facility_address,
        fd.workspace_name,
        fd.team_name,
        fd.team_domain

    from aerial_maps am
    left join facility_details fd
        on am.region_facility_id = fd.region_facility_id
),

-- 2단계: 계산 컬럼 생성
enriched as (
    select
        *,

        -- [OUTPUT_OPTION 플래그] stg에서 이미 처리됨 (is_orthomosaic, is_pointcloud, is_mesh, is_dsm)

        -- [IMAGE_RESOLUTION] WxH 문자열
        case
            when image_resolution_width is not null and image_resolution_height is not null
            then cast(image_resolution_width as string) || 'x' || cast(image_resolution_height as string)
            else null
        end as image_resolution,

        -- [PROCESS_TIME] uploaded_at ~ published_at (분 단위)
        case
            when uploaded_at is not null and published_at is not null
            then timestamp_diff(published_at, uploaded_at, minute)
            else null
        end as process_time_minutes,

        -- [PROCESSING_TIME_PER_IMAGE] process_time / image_count (분 단위)
        case
            when uploaded_at is not null
                and published_at is not null
                and image_count > 0
            then timestamp_diff(published_at, uploaded_at, minute) / image_count
            else null
        end as processing_time_per_image_minutes,

        -- [RMS_ERROR_XYZ_GROUP] 구간 분류
        case
            when rms_error_xyz is null then null
            when rms_error_xyz <= 0.5 then '<= 0.5m'
            when rms_error_xyz <= 1.5 then '0.5~1.5m'
            when rms_error_xyz <= 3.0 then '1.5~3m'
            when rms_error_xyz <= 6.0 then '3~6m'
            else '> 6m'
        end as rms_error_xyz_group,

        -- [비정상 데이터 여부]
        case
            when not is_orthomosaic and not is_pointcloud and not is_mesh and not is_dsm then true
            when uploaded_at is null then true
            else false
        end as is_abnormal,

        -- [내부 데이터 여부]
        case
            when team_domain in (
                'updatedemo', 'qatest', 'hodulee', 'finephone2',
                'demokr', 'demo', 'cupix', 'wwtest', 'qatest3'
            ) then true
            else false
        end as is_internal,

        -- [Image Giga Pixel 총합]
        case
            when image_resolution_width is not null
                and image_resolution_height is not null
                and image_count > 0
            then image_resolution_width * image_resolution_height / 1000000000.0 * image_count
            else null
        end as image_giga_pixel_total,

        -- [프로세싱 비용 (USD)]
        case
            when image_resolution_width is not null
                and image_resolution_height is not null
                and image_count > 0
            then image_resolution_width * image_resolution_height / 1000000000.0 * image_count * 13200 / 1450
            else null
        end as processing_cost_usd

    from joined
),

-- 3단계: CAPTURE_CADENCE 계산 (facility별 캡처 주기)
cadence as (
    select
        region_facility_id,
        case
            when count(*) <= 1 then 0
            else
                timestamp_diff(max(captured_at), min(captured_at), day)
                / cast(count(*) as float64)
        end as capture_cadence_days
    from enriched
    where captured_at is not null
    group by region_facility_id
),

-- 4단계: cadence 합치기
final as (
    select
        e.*,
        coalesce(c.capture_cadence_days, 0) as capture_cadence_days

    from enriched e
    left join cadence c
        on e.region_facility_id = c.region_facility_id
)

select * from final