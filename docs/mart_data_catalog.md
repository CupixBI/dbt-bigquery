# CupixBI Mart Data Catalog

BigQuery 프로젝트: `prism-485708`  
Mart 스키마: `prod_03_marts`

> 이 문서는 Claude chat에서 대시보드 설계, 지표 탐색, SQL 작성 시 context로 활용됩니다.
> 모든 테이블은 `prism-485708.prod_03_marts.<table_name>` 으로 조회합니다.

---

## 도메인 분류

| 도메인 | 테이블 |
|--------|--------|
| **Capture Processing** | fct_capture_processing_enriched, fct_capture_processing_inflow_outflow, fct_capture_processing_stage_duration, mart_capture_processing_stage_duration_2 |
| **CQA (편집 품질)** | fct_cqa_shift_hours, fct_editor_weekly_activity, fct_project_capture_processing_performance, fct_sla_violation_territory |
| **BIM** | mart_bim_pipeline, mart_bim_level_histogram, mart_bim_quality_analysis |
| **SQA / Element Trace** | mart_element_trace_sqa_throughput, mart_element_trace_volume_trend, mart_ai_trust_sqa_change_rate |
| **Sales / Revenue** | mart_sales_opportunity, mart_nrr, mart_growth_mrr_monthly, mart_at_risk_revenue, mart_customer_value_maturation, mart_cost_profitability |
| **운영** | fct_team_summary, fct_license_usage, mart_event_weekly, mart_leads, mart_si_processing |

---

## Capture Processing

### `fct_capture_processing_enriched`
캡처 처리 전체 파이프라인의 핵심 팩트 테이블. 캡처 단위로 모든 처리 단계 타임스탬프, 소요시간, 플래그를 포함.

**주요 Dimension**
| 컬럼 | 설명 |
|------|------|
| region_capture_id | 캡처 고유 ID (region prefix 포함) |
| region | uswe2(US), apse2(AU), euce1(EU), apne1(JP), apse1(SG), cace1(CA) |
| tenant | 테넌트 |
| capture_type | Drive / Photo / 3D Map / Video |
| video_length_range | ~5min / 5~10min / 10~20min / 20min~ |
| team_name, region_team_id | 팀 |
| project_name, project_id | 프로젝트 (facility) |
| editor_email, editor_name | 담당 에디터 |
| editor_level | 에디터 레벨 |
| editor_work_part | 에디터 업무 파트 |
| editing_state | 편집 상태 |
| region_level_id | 레벨 ID |
| camera_model_name | 카메라 모델 |
| refinement_floorplan_type | Refinement 도면 타입 |
| cqa_refinement_selected_type | CQA refinement 선택 타입 |

**주요 Timestamp (KST 버전도 존재: _kst suffix)**
| 컬럼 | 설명 |
|------|------|
| uploading_finished_at | 업로드 완료 시각 |
| editing_created_at | 편집 큐 생성 시각 |
| edit_started_at | 편집 시작 시각 |
| edit_finished_at | 편집 완료 시각 |
| review_started_at | 리뷰 시작 시각 |
| review_finished_at | 리뷰 완료 시각 |
| reconstruction_started/finished_at | CPC 생성 시작/완료 |

**주요 Duration (분 단위)**
| 컬럼 | 설명 |
|------|------|
| total_lead_time_min | 업로드 완료 → 최종 완료 총 소요시간 |
| edit_started_to_edit_finished_min | 순수 편집 소요시간 |
| uploading_to_processing_finished_min | 업로드 → 처리 완료 |
| review_started_to_review_finished_min | 리뷰 소요시간 |

**주요 Flag / Count**
| 컬럼 | 설명 |
|------|------|
| is_sla_exceeded | SLA 초과 여부 (bool) |
| is_holded | 홀드 여부 |
| is_escalated | 에스컬레이션 여부 |
| has_review | 리뷰 단계 존재 여부 |
| has_cpc | CPC(3D reconstruction) 존재 여부 |
| is_recalculated | 재처리 여부 |
| edit_process_count | 편집 횟수 |
| floorplan_count | 도면 수 |
| bim_dwg_count | BIM DWG 도면 수 |
| sub_clusters_count | Sub cluster 수 |
| bims_count | BIM 파일 수 |

---

### `fct_capture_processing_inflow_outflow`
날짜별 캡처 유입/유출 흐름 추이. 적체 분석에 사용.

| 컬럼 | 설명 |
|------|------|
| event_date | 날짜 |
| inflow_count | 유입 캡처 수 |
| outflow_count | 유출(완료) 캡처 수 |
| net_flow | 유입 - 유출 |
| cumulative_difference | 누적 적체량 |
| inflow_length_min / outflow_length_min | 영상 길이 기준 유입/유출 |

---

### `fct_capture_processing_stage_duration`
캡처 처리 7단계별 소요시간 (long format). 단계별 병목 분석에 사용.

| 컬럼 | 설명 |
|------|------|
| stage | 1~7단계 (Upload→Processing, Processing, Preview, Queue, Edit, Review, CPC) |
| duration_min | 해당 단계 소요시간 (분) |
| capture_type | 캡처 타입 |
| team_name | 팀명 |
| video_length_range | 영상 길이 구간 |

---

### `mart_capture_processing_stage_duration_2`
stage_duration과 유사하나 초(sec) 단위, region_capture_id 포함.

---

## CQA (편집 품질 관리)

### `fct_editor_weekly_activity`
에디터별 주간 활동 지표.

| 컬럼 | 설명 |
|------|------|
| editor_email | 에디터 이메일 |
| week | 주 시작일 (월요일 기준) |
| active_days | 활동 일수 |
| capture_count | 처리 캡처 수 |
| fte | active_days / 5 (풀타임 등가) |
| sla_exceeded_count | SLA 초과 캡처 수 |
| sla_exceeded_pct | SLA 초과율 |

---

### `fct_cqa_shift_hours`
CQA 에디터 근무 스케줄 (시간 단위 전개).

| 컬럼 | 설명 |
|------|------|
| editor_name | 에디터 이름 |
| work_part | 업무 파트 (예: MF06, MF20) |
| editor_level | 레벨 |
| location | 근무 지역 |
| day_num | 요일 (1=월 ~ 7=일) |
| hour | 시각 (0~23) |

---

### `fct_project_capture_processing_performance`
프로젝트별 처리 품질 성과 지표.

| 컬럼 | 설명 |
|------|------|
| team_name, project_name, project_id | 팀/프로젝트 |
| captures | 총 캡처 수 |
| delivery_sla_rate | SLA 준수율 |
| rework_rate | 재작업률 |
| recalculation_rate | 재처리율 |
| avg_edit_time_per_1min_video | 영상 1분당 평균 편집 시간 |
| error_rate | 오류율 |
| status | TARGET / WARNING / ACTION NEEDED |

---

### `fct_sla_violation_territory`
리전별 캡처 타입별 SLA 위반율.

| 컬럼 | 설명 |
|------|------|
| region | 리전 |
| sla_breach_rate_drive/photo/3d_map/area/video | 타입별 SLA 위반율 |

---

## BIM

### `mart_bim_pipeline`
캡처 단위 BIM 파이프라인 현황. BIM 도입 → Refinement 흐름 추적.

| 컬럼 | 설명 |
|------|------|
| region_capture_id | 캡처 ID |
| region_facility_id, project_name | 프로젝트 |
| is_bim_facility / is_bim_level | BIM 시설/레벨 여부 |
| bims_count, bim_dwg_count | BIM/DWG 수 |
| is_captured_after_bim_created | BIM 생성 후 캡처 여부 |
| is_dwg_selected | DWG 선택 여부 |
| refinement_result_exists | Refinement 결과 존재 여부 |
| is_refinement_adopted | Refinement 채택 여부 |

---

### `mart_bim_level_histogram`
레벨 단위 BIM Refinement 퍼널.

| 컬럼 | 설명 |
|------|------|
| region_level_id | 레벨 ID |
| after_bim_capture_count | BIM 이후 캡처 수 |
| dwg_selected_count / dwg_selection_rate | DWG 선택 수/율 |
| refinement_success_count / refinement_success_rate | Refinement 성공 수/율 |
| refinement_adopted_count / refinement_adoption_rate | Refinement 채택 수/율 |

---

### `mart_bim_quality_analysis`
레코드 단위 BIM 품질 집계.

---

## SQA / Element Trace

### `mart_element_trace_sqa_throughput`
날짜별 Element Trace SQA 처리량.

| 컬럼 | 설명 |
|------|------|
| date / date_kst | 날짜 |
| region, tenant | 리전/테넌트 |
| status_name | 상태명 |
| trust_level | AI 신뢰 레벨 |
| element_trace_count | element trace 수 |
| type | created / sqa_processed |

---

### `mart_element_trace_volume_trend`
Element Trace 생성/업데이트 추이.

---

### `mart_ai_trust_sqa_change_rate`
AI trust level별 SQA 변경률.

| 컬럼 | 설명 |
|------|------|
| created_date_kst | 날짜 |
| trust_level | AI 신뢰 레벨 |
| total_count | 전체 건수 |
| sqa_changed_count / sqa_change_rate | SQA 변경 건수/율 |

---

## Sales / Revenue

### `mart_sales_opportunity`
Salesforce Opportunity 기반 영업 현황.

| 컬럼 | 설명 |
|------|------|
| opportunity_id, opportunity_name | 기회 ID/명 |
| amount_usd | 금액 (USD) |
| stage_name | 영업 단계 |
| close_date | 클로즈 날짜 |
| contract_type | 계약 타입 |
| market_segment | 시장 세그먼트 |
| account_name, territory | 계정/지역 |
| is_closed_won | 성사 여부 |
| owner_name, owner_region | 담당자 |

---

### `mart_nrr`
월별 계정별 NRR(Net Revenue Retention) 구성요소.

| 컬럼 | 설명 |
|------|------|
| month_start, year_month | 기간 |
| account_id, account_name | 계정 |
| active_mrr | 활성 MRR |
| new_mrr / renewal_mrr / expansion_mrr | 신규/갱신/확장 MRR |
| contraction_mrr / churn_mrr | 축소/이탈 MRR |

---

### `mart_growth_mrr_monthly`
월별 Opportunity 기반 MRR 성장 추이.

| 컬럼 | 설명 |
|------|------|
| month_start, year_month | 기간 |
| monthly_mrr | 월 MRR |
| market_segment, territory | 세그먼트/지역 |
| account_name | 계정 |

---

### `mart_at_risk_revenue`
이탈 위험 Revenue 현황.

| 컬럼 | 설명 |
|------|------|
| at_risk_stage | Expiring / Early At-Risk / Late At-Risk |
| months_to_end / months_since_end | 만료까지/만료 후 개월 수 |
| opp_mrr | MRR |
| account_name, ae_name, csm_name | 계정/AE/CSM |

---

### `mart_customer_value_maturation`
계정별 고객 가치 성숙도.

| 컬럼 | 설명 |
|------|------|
| total_spending_usd | 총 누적 지출 |
| tier | 금액 기준 고객 등급 |
| ytd_spending_usd | 연도별 지출 |
| ytd_yoy_pct | YoY 성장률 |

---

### `mart_cost_profitability`
계정별 월별 비용/수익성.

| 컬럼 | 설명 |
|------|------|
| year_month | 기간 |
| total_mrr | MRR |
| processing_cost | 처리 비용 |
| editing_labor_cost | 편집 인건비 |
| total_cost | 총 비용 |
| gross_profit | 매출총이익 |
| gross_margin_pct | 매출총이익률 |

---

## 운영

### `fct_team_summary`
팀별 운영 현황 종합.

| 컬럼 | 설명 |
|------|------|
| team_name, region | 팀/리전 |
| annual_revenue | 연간 매출 |
| active_workspaces / active_facilities | 활성 워크스페이스/시설 수 |
| used_area_sqft | 사용 면적 |
| area_utilization_rate | 면적 활용률 |
| capture_error_rate | 캡처 오류율 |
| last_activity_at | 마지막 활동 시각 |

---

### `fct_license_usage`
라이선스별 사용 현황.

| 컬럼 | 설명 |
|------|------|
| team_name, workspace_name | 팀/워크스페이스 |
| license_label | 라이선스 레이블 |
| license_status | 상태 (활성/만료 등) |
| capacity_utilization_rate | 용량 활용률 |
| days_until_expiry | 만료까지 남은 일수 |
| is_pilot | 파일럿 여부 |

---

### `mart_event_weekly`
주별 사용자 이벤트 활동.

| 컬럼 | 설명 |
|------|------|
| week | 주 |
| user_email | 사용자 |
| event_name | 이벤트명 |
| event_count | 이벤트 수 |
| team_name, workspace_name | 팀/워크스페이스 |

---

### `mart_leads`
Salesforce Lead 현황.

| 컬럼 | 설명 |
|------|------|
| lead_name, lead_email | 리드 |
| status, rating | 상태/등급 |
| market_segment | 시장 세그먼트 |
| is_converted | 전환 여부 |
| days_to_conversion | 전환 소요일 |
| days_since_last_activity | 마지막 활동 후 경과일 |

---

### `mart_si_processing`
Site Insights 처리 현황.

| 컬럼 | 설명 |
|------|------|
| cqa_duration_min | CQA 소요시간 |
| sitetrack_duration_min | Sitetrack 소요시간 |
| sqa_processing_duration_min | SQA 처리 소요시간 |
| reviewer_email | 리뷰어 이메일 |
