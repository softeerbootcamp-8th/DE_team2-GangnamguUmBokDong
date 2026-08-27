# rebalance 파이프라인 시각자료

대여소별 대여·반납 수요 예측이 어떻게 **긴급도 점수**가 되고, 다시 **트럭 한 대의 방문 순서**가 되는지를
단계별로 나눈 그림입니다. 각 이미지는 `.svg`(벡터, 라이트/다크 자동 전환)와 `.png`(2x 래스터) 두 형태로 있습니다.

| # | 이미지 | 다루는 구간 | 관련 코드 |
|---|--------|------------|-----------|
| MAP | [00-overview](images/00-overview.png) | 배치 두 개와 그 사이의 parquet, RDS를 건드리는 두 지점 | `main.py`, `routes_main.py` |
| 01 | [01-demand-to-stock](images/01-demand-to-stock.png) | 대여·반납 수요 → 예측 재고 곡선 → `action_type` | `core/forecast.py:enrich_forecast_points` |
| 02 | [02-urgency-timing](images/02-urgency-timing.png) | 시급성 — 세 감지기 → `min()` → `time_factor` | `urgency.py:_trend_time_to_critical`, `_forecast_time_to_critical` |
| 03 | [03-urgency-severity](images/03-urgency-severity.png) | 심각도 — 세 가지 자 → `impact_factor` | `urgency.py:_severity_qty`, `_severity` |
| 04 | [04-score-and-qty](images/04-score-and-qty.png) | 점수 조립과 `bike_qty` 클램프 | `urgency.py:urgency_score`, `bike_qty` |
| 05 | [05-routes-scope](images/05-routes-scope.png) | dispatched 넷팅, 11개 권역 배정 | `routes.py:_remaining_need`, `core/regions.py` |
| 06 | [06-routes-truck](images/06-routes-truck.png) | 용량 20대 그리디 적재, 최근접 방문 순서 | `routes.py:_select_up_to_capacity`, `_nearest_neighbor_order` |
| SUM | [07-recap](images/07-recap.png) | 전체 흐름 한 장 요약 | — |

## 그림 속 수치에 대해

01·03·04에 나오는 두 대여소 예시는 실제 `urgency.py`에 입력을 넣어 얻은 값입니다.

| | 대여소 A | 대여소 B |
|---|---|---|
| 정원 / 현재 재고 | 15대 / 12대 | 20대 / 6대 |
| `action_type` | `retrieval_needed` | `supply_needed` |
| `_max_deficit` | — | 0대 |
| `_max_unmet_demand` | — | 10대 |
| `urgency_score` | 53.0 | 23.1 |
| `bike_qty` | 12대 회수 | 10대 공급 |

대여소 B는 순누적 부족분이 0인데도 점수가 붙는 케이스로, `_max_unmet_demand`가 없으면
이번 배치에서 아예 보이지 않습니다.

05의 지역센터 배치는 `core.regions.DISPATCH_CENTERS`의 실제 위경도를 그대로 투영한 것이고,
그 위의 대여소 위치는 설명을 위한 예시입니다.

## 다시 생성하려면

이미지는 아티팩트 페이지에서 추출한 SVG를 자립형 파일로 재발행한 것입니다.
좌표·라벨 보정은 `patches.py`에, 조립 로직은 `gen.py`에 있습니다(둘 다 저장소 밖 작업 파일).
PNG는 headless Chrome으로 SVG를 2x 스크린샷해 만들었습니다.
