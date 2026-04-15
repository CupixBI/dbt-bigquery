# CupixBI dbt-bigquery 프로젝트

## 프로젝트 개요
- BigQuery 프로젝트: `prism-485708`
- dbt Cloud CLI 사용 (프로필: `CupixPrism`, host: `uo220.us1.dbt.com`)
- 레이어: staging(bronze) → intermediate(silver) → marts(gold)

## 실행 명령어
```powershell
# 컴파일만 (SQL 오류 체크)
dbt compile --select <model_name>

# 실행 (기본이 dev 환경)
dbt run --select <model_name>

# 여러 모델 동시 실행
dbt run --select stg_tesla__clusters stg_tesla__floorplans fct_capture_processing_enriched
```

## 소스 구성 (`models/staging/_sources.yml`)
| source name  | database       | schema           | 주요 테이블 |
|-------------|----------------|------------------|------------|
| tesla        | prism-485708   | tesla_raw        | captures, clusters, floorplans, levels, facilities, teams, users, editings, cameras, ... |
| cupixworks   | prism-485708   | cupixworks_raw   | capture_traces, segment_events |
| monday       | prism-485708   | monday_raw       | capture_issues_native |
| slack        | prism-485708   | slack_raw        | re_edit_requested_native |
| finance      | prism-485708   | finance_raw      | cqa_editors_native, cqa_shift_schedule_native, ... |
| salesforce   | prism-485708   | salesforce_raw   | opportunity, account, lead, ... |
| seed         | prism-485708   | seed_raw         | team_sf_account_mapping |

## 스테이징 모델 패턴 (`stg_tesla__*.sql`)

모든 스테이징 모델은 **source → renamed → final** 3단계 CTE 구조를 사용한다.

```sql
WITH source AS (
    SELECT * FROM {{ source('tesla', '<table>') }}
),

renamed AS (
    SELECT
        region,
        CAST(_id AS STRING) AS <entity>_id,   -- PK는 항상 _id, STRING으로 캐스팅
        CAST(<fk_id> AS FLOAT64) AS <fk>_id,  -- FK도 STRING 캐스팅
        <other_columns>,
        tenant
    FROM source
),

final AS (
    SELECT
        region,
        <entity>_id,

        -- region prefix 패턴 (모든 ID에 적용)
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
            <entity>_id
        ) AS region_<entity>_id,

        <other_columns>,
        tenant
    FROM renamed
)

SELECT * FROM final
```

**핵심 규칙:**
- PK: `_id` 컬럼을 `STRING`으로 캐스팅 → `<entity>_id`
- 조인 키: `region_<entity>_id` = region prefix + `-` + id (예: `US-12345`)
- region 값: `uswe2`=US, `apse2`=AU, `euce1`=EU, `apne1`=JP, `apse1`=SG, `cace1`=CA
- NESTED STRUCT 필드는 `renamed` 단계에서 바로 추출 (예: `meta.preview.field IS NOT NULL AS has_xxx`)

## 주요 모델 계보
```
stg_tesla__captures
stg_tesla__clusters      ─┐
stg_tesla__floorplans    ─┤
stg_tesla__levels        ─┤
stg_tesla__facilities    ─┤→ int_capture_details ─┐
stg_tesla__teams         ─┤                        ├→ fct_capture_processing_enriched
stg_tesla__users         ─┤                        │
stg_finance__cqa_editors ─┘   int_capture_processing ─┘
```

## `fct_capture_processing_enriched` 컬럼 메모
- `refinement_floorplan_type`: `stg_tesla__captures` → `int_capture_details` 경유
- `cqa_refinement_selected_type`: clusters.skat_result_type, capture 단위 MIN 집계
- `sub_clusters_count`: clusters에서 kind='sub'인 것만 COUNTIF, capture 단위
- `floorplan_count`: floorplans COUNT(*), level 단위 집계 후 capture에 조인
- `bim_dwg_count`: floorplans에서 floorplan_type='bim'인 것만 COUNTIF, level 단위
- `refinement_result_exists`: clusters.meta.preview.align_preview_meta_refinement_with_prior_map IS NOT NULL, capture 단위 COUNTIF > 0

## BigQuery 문법 참고
- `COUNTIF(조건)` — Snowflake의 `COUNT(IFF(조건, 1, NULL))` 대응
- STRUCT 필드 접근: `meta.preview.field_name`
- 집계 후 boolean: `COUNTIF(조건) > 0 AS flag`
