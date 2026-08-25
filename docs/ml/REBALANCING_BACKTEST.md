# 재배치 시스템 point-in-time 백테스트

> **문서 상태:** 현재 평가 계약은 `point-in-time-policy-backtest-v3`, 운영 경로
> 알고리즘은 `route-v4-supply-led-pickup-sla`, 긴급도 계산 설정은
> `urgency-scoring-v4-any-depletion`이다. 이 문서의 12셀 수치는 후보 교정
> (calibration) 결과이며, 별도로 사전 등록한 confirmatory matrix는 아직 실행하지
> 않았다. 원천 CSV, 모델 bundle, MinIO station master와 생성 결과는 Git에 포함하지
> 않는다.

## 현재 production 후보

백테스트와 Gold publication이 함께 고정하는 후보 이름과 정책 버전은 다음과 같다.

```text
policy=production_route_v4
route_algorithm_version=route-v4-supply-led-pickup-sla
urgency_scoring_config_version=urgency-scoring-v4-any-depletion
rebalance_policy_version=rebalance-risk-band-v4-any-depletion-h2-r0.20-z1.645-f0300bp-cooldown120-exclusive1
```

정책 설정의 exact 의미는 다음과 같다.

- 수량 전략: `risk_band`
- 예측 보호 구간: 2시간
- 최소 재고: 정원의 20%
- 불확실성 계수: `z=1.645`, donor 회수량에만 적용(`pickup_only`)
- 한 판단의 회수 상한: 현재 재고의 3%(`f0300bp`)
- donor 보호: 최근 실측이나 모델 평균 중 하나라도 감소하면 회수하지 않는
  `any-depletion-v1`
- 같은 donor를 진행 중인 여러 경로가 함께 회수하지 않는 exclusive pickup
- 배차한 donor의 재회수 cooldown: 120분
- 한 경로의 최대 방문 대여소: 5곳
- 센터별 한 tick의 최대 신규 경로: 3개

계획 시 계산한 양수 수량이 production 작업 지시다. 시뮬레이터에만 존재하는 도착
시점 안전재고 clamp로 결과를 좋게 만들지 않으며, 실행 단계에서는 실제 재고·거치
공간·트럭 적재량이라는 물리 한계만 적용한다.

## 무엇을 평가하는가

평가는 과거 실측 시작 재고에서 당시까지 관측할 수 있었던 입력으로 모델을 5분마다
다시 실행하고, 시민 대여 요청과 트럭 작업을 시간순으로 재생한다. 비교 기준은 같은
관측 요청을 트럭 재배치 없이 재생한 `no_rebalance`다. 미래 실제 수요로 이동량을
만드는 oracle 실험은 경로 디버깅에만 사용하고 성능 근거에는 포함하지 않는다.

Primary metric은 코드 상수와 같은 `observed_demand_fulfillment_rate`다.

```text
observed_demand_fulfillment_rate
    = replay에서 성공한 관측 대여 요청 수 / 관측 대여 요청 수
```

여기서 **관측 요청은 현실에서 성공해 대여 이력에 남은 trip**이다. 현실에서 자전거가
없어서 시도하지 않았거나 실패해 기록되지 않은 잠재 수요는 분모에 없다. 또한 replay
정책에서 대여가 실패하면 그 자전거의 후속 반납도 제거한다. 따라서 이 지표는 고정된
성공 trip 집합을 정책이 얼마나 보존하는지 보는 반사실 proxy이며, 실제 전체 시민
수요의 절대 충족률이나 기존 운영 대비 인과적 우월성을 뜻하지 않는다.

보조 지표인 `empty_station_minutes`는 평가 창에서 재고가 0인 대여소-분의 합이다.
충족률 변화가 몇 건 단위라 작아 보여도, 품절 상태를 얼마나 오래 줄였는지를 함께
확인한다. 요청 단위 no-harm을 확인하기 위해 baseline에서는 성공했는데 후보에서 새로
실패한 event 집합도 별도로 비교한다.

기존 운영자의 station별 순개입은 각 정시 구간에서 다음 잔차로 추정할 수 있다.

```text
운영 잔차 = 다음 실측 재고 - (현재 실측 재고 - 시민 대여 + 시민 반납)
```

공개 데이터에는 실제 경로·트럭·작업 시각이 없으므로 잔차를 구간 초·중·말에 적용한
세 경우를 모두 기록한다. 이 추정치는 설명용 비교면이지 실제 운영과의 인과 비교
근거는 아니다.

## 점수, action, `bike_qty`의 관계

긴급도 단계는 각 대여소에 다음 세 값을 함께 게시한다.

- `rebalance_need_type_cd`: `supply_needed`, `retrieval_needed`, `normal` 중 판단
- `urgency_score`: 같은 action 후보 안에서 먼저 처리할 대여소를 정하는 우선순위
- `bike_qty`: 현재 안전하게 실행 가능한 이동 수량

`action`과 `bike_qty`는 같은 뜻이 아니다. 예를 들어 예측상 회수가 필요해
`retrieval_needed`여도, 현재 정원 초과분이 없거나 donor 안전 조건을 통과하지 못하면
`bike_qty=0`일 수 있다. 이 행은 판단·관측 결과로는 남지만 지금 실행할 양수 작업이
없으므로 route 후보에서 제외한다. `normal`도 항상 수량 0이라 route 후보가 아니다.
즉 “대여소를 평가 대상에서 삭제”하는 것이 아니라 “이번 tick에는 0대를 옮기는 stop을
만들지 않는다”는 의미다.

반대로 현재 재고가 0이고 예측 대여·반납도 모두 0인 대여소는 현재 재고가 정원의
20% 이하이므로 `supply_needed`다. 평균 예측 경로도 0이라면 `bike_qty`는 물리적 빈
거치 공간 안에서 최소 재고 20%까지 채우는 양수가 된다. 다만 예측 수요로 계산한
심각도가 낮아 score가 0일 수 있으므로 수요가 실제로 예상되는 공급처보다 우선도는
낮다. 양수 donor와 경로 여유가 있어야 실제 route에 포함된다.

회수와 공급의 평균 score가 달라도 회수만으로 경로를 만들지는 않는다. score는
pickup끼리, dropoff끼리의 순서를 정하는 입력이고, route는 항상 pickup 합계와
dropoff 합계가 같은 완결 작업만 선택한다. 현행 경로는 최고 우선순위 supply가 route
ordinal과 첫 dropoff를 소유하는 supply-led 구조다. 따라서 회수 score 평균이 높다는
이유만으로 회수 stop만 잡는 전역 비교는 없다.

## Risk-band 실행 수량

현재 재고를 `current`, 정원을 `capacity`, 최소 재고를 `minimum_stock`이라 한다.

```text
minimum_stock = ceil(capacity * 0.20)
```

### Pickup: donor 보호가 먼저다

2시간 모델 평균 경로의 어느 시점이라도 현재보다 감소하거나 최근 실측 회귀 기울기가
음수이면 해당 donor의 pickup 수량은 0이다. 최근 실측은 현재 anchor를 포함해 서로
다른 시각이 최소 3개 있어야 하며, 부족하면 fail-closed한다. 통과한 donor만 독립
포아송 대여·반납 근사의 누적 분산에 `z=1.645`를 적용한 모델 하방과 최근 실측을
2시간+출동 30분까지 외삽한 하방 중 더 보수적인 값을 쓴다.

```text
current_surplus = max(0, current - capacity)
safe_surplus = max(
    0,
    floor(min(model_uncertainty_lower, recent_projection_lower) - minimum_stock),
)
concentration_limit = floor(current * 0.03)

pickup_qty = min(current, current_surplus, safe_surplus, concentration_limit)
```

핵심은 미래 반납으로 생길 초과량을 지금 빌려 회수하지 않는다는 점이다. **현재 정원을
초과한 자전거만** donor가 될 수 있고, 그중에서도 최근·예측 어느 쪽에도 고갈 신호가
없으며 회수 후 20% 안전재고를 지키는 3% 이하만 가져간다. `z=1.645`는 이 donor
하방에만 적용된다.

### Dropoff: 평균 경로를 최소 재고까지 보충한다

공급은 2시간 평균 예측 재고 경로의 최저점을 최소 재고까지 올리되, 현재 빈 거치
공간을 넘지 않는다.

```text
dropoff_qty = min(
    max(0, ceil(minimum_stock - min(model_mean_stock_path))),
    max(0, capacity - current),
)
```

공급량에는 `z=1.645`를 추가하지 않는다. 즉 불확실성 완충은 회수 측 donor 보호에만
쓰고, 공급 측은 평균 경로와 20% 최소 재고로 계산한다.

## Supply-led route-v4

각 트럭은 센터에서 빈 적재량으로 출발하므로 pickup 뒤 dropoff 순서만 허용한다.
모든 stop prefix에서 적재량은 0~20대이고 한 경로의 pickup 합계와 dropoff 합계는
exact하게 같다.

1. 센터별 score가 가장 높은 양수 supply가 route anchor와 ordinal을 소유한다.
2. donor는 `센터→donor→supply anchor` 총거리, 센터 도착거리, 긴급도 순으로 고른다.
3. pickup을 모두 실행한 뒤 anchor를 첫 dropoff로 방문하고, 추가 공급처는
   긴급도-거리 효율과 최근접 순서로 방문한다.
4. 트럭 용량 20대, 경로당 최대 5개 대여소, 센터당 한 tick 최대 3개 경로 안에서
   이동량이 가장 큰 완결 pickup/dropoff split을 선택한다.
5. 진행 중 경로의 수량은 coverage에서 차감한다. exclusive pickup과 120분 cooldown으로
   같은 donor의 중복·연속 회수를 막는다.
6. 단일 pickup도 센터 출발 후 실행까지 30분을 넘으면 제외한다. 여러 pickup은
   20km/h 직선거리와 대여소당 3분 작업을 누적해 마지막 pickup 실행이 30분 이내인지
   검증한다.
7. 큰 split이 pickup SLA를 넘으면 더 작은 완결 split을 선택한다. 앞의 먼 donor가
   불가능해도 뒤의 가까운 donor를 함께 미루지 않도록 SLA 가능 후보를 안정적으로
   앞으로 모은다.

30분 상한은 “경로 전체 완료”가 아니라 **dispatch부터 마지막 pickup 실행까지**의
donor 보호 SLA다. 평가 종료 전에 모든 stop과 센터 복귀가 끝날 수 있는 경로만
배차하는 cutoff 계약은 별도로 적용한다.

## Point-in-time 평가 계약

기본 근거 실행은 다음 조건을 고정한다.

- 운영 판단 주기: 5분
- 평가 구간: 60분·120분·180분을 한 묶음으로 실행
- 트럭: 권역당 3대, 용량 20대
- 주행 근사: 20km/h, 대여소당 작업 3분
- 승인 지연: 자동 정책 평가를 위해 0분
- 배차 cutoff: 종료 전에 마지막 작업과 센터 복귀가 가능한 경로만 시작
- 공통 이동 예산: 같은 구간의 기존 운영 잔차 중 `min(유입, 유출)`
- 진행 중 작업 전체를 다음 tick의 coverage로 차감하고, 센터 복귀 전까지 트럭 점유

120분은 모델 예측 범위가 아니라 현장 작업 단위를 함께 검토하기 위해 사전에 정한
구간이다. 특정 결과가 좋아 보이는 길이를 사후 선택하지 않도록 근거 실행은
60·120·180분을 모두 요구한다.

정보시점은 다음과 같이 고정한다.

- 시작 재고: 평가 시작 정시의 실측값만 사용하고 이후에는 정책별 사건 재생 상태만
  사용한다.
- 대여소 식별자: 월 대여 이력 공공 번호와 내부 `ST-<suffix>`의 1:1 crosswalk를
  만들며 다대일·일대다는 fail-closed한다.
- 대여 lag: `[T-100분, T-40분)`에 시작하고 T까지 반납된 성공 이용만 쓴다.
- 반납 lag: `[T-60분, T)`의 성공 반납만 쓴다.
- 날씨: anchor보다 60분 이전인 최신 관측만 사용한다.
- 생활인구: 대상일보다 1~4주 전 자료를 0.4/0.3/0.2/0.1로 가중한다. 모두 결측인
  셀만 5~8주 전 최근값으로 보완하며, 끝까지 결측인 격자는 제외한다.
- 모델: `aws-temporary-model-2025-d20-h12-r20` 원본 bytes를 SHA-256으로 고정한다.
  매월 17일은 학습에서 제외된 test split이다.

결과에는 원천 대여·재고·날씨·생활인구의 경로·크기·SHA-256, station master와
crosswalk SHA-256, 모델 SHA-256, 정책·경로·scoring·backtest semantic version을
함께 기록한다.

## 최신 develop 기반 12셀 calibration 결과

후보를 고르는 과정에서 열람한 권역과 시간대 12셀을 current route-v4로 다시 실행했다.
raw 위치는 실행 중인 scheduler container의
`/tmp/rebalance-v4-route-sla-calibration-latest-develop-20260825-r1`이며, Git
산출물이 아니다.

| 센터 | 평가 셀(날짜 시각, Asia/Seoul) |
|---|---|
| gaehwa | 2025-04-17 06:00, 2025-07-17 12:00, 2025-08-17 20:00, 2025-12-17 17:00 |
| isu | 2025-04-17 17:00, 2025-07-17 06:00, 2025-08-17 20:00, 2025-12-17 12:00 |
| yeongnam | 2025-04-17 12:00, 2025-07-17 17:00, 2025-08-17 20:00, 2025-12-17 06:00 |

12개 raw JSON의 36개 구간을 fail-closed validator로 검증한 aggregate는 다음과 같다.
품절 시간의 셀 결과는 `개선/동률/악화` 순서다.

| 구간 | 관측 요청 | 미충족(no rebalance→후보) | 충족률 변화 | 품절 대여소-분(no rebalance→후보) | 감소율 | 셀 결과 | 계획=실행 | 배차=완료 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 60분 | 6,663 | 1→0 | +0.015008pp | 10,626.150→10,442.809 | -1.725% | 6/6/0 | 70=70 | 31=31 |
| 120분 | 14,995 | 11→10 | +0.006669pp | 21,440.383→20,332.428 | -5.168% | 10/2/0 | 159=159 | 69=69 |
| 180분 | 22,713 | 32→29 | +0.013208pp | 32,609.084→30,074.233 | -7.773% | 11/1/0 | 241=241 | 106=106 |

180분 관측 수요 충족률 원값은 `99.8591115%→99.8723198%`다. 모든 셀·구간에서
baseline 성공 요청이 후보 때문에 새로 실패한 event는 0건이었다. 모든 배차는 cutoff
안에 완료됐고 종료 시 busy truck은 0대였다. 전체 raw에서 dispatch부터 pickup
실행까지 최대 지연은 `29.196398분`으로 30분 SLA 이내였다. 12개 raw의 route·scoring·
policy·model provenance도 exact하게 일치했다.

이 결과는 후보를 개선하는 데 사용한 **calibration 근거**다. 전 구간에서 미충족과
품절 시간이 악화되지 않았고 180분 품절 시간이 7.773% 줄었다는 것은 확인했지만,
독립 confirmatory 결과로 부르거나 production gate 통과를 주장하지 않는다.

이전 회수 상한 후보 비교 결과는 현행 any-depletion·pickup SLA 정책보다 앞선
superseded calibration이다. 현재 정책의 근거나 현재 production 설정으로 재사용하지
않으며 필요하면 Git history에서만 확인한다.

## 사전 등록 confirmatory matrix v3

`confirmatory-matrix-v3.json`은 v2에서 고정한 selection, 12셀, 60·120·180분 평가 계약,
acceptance gate를 바꾸지 않고 최신 develop base ancestry와 후보 semantic version을
추가로 잠근 manifest다.

- Manifest SHA-256:
  `3d31517d57e3a460f1c9d0acc9637f5a91beaffcb6936514c61026263386ca15`
- 등록 branch: `feature/rebalance-policy-v3`
- 등록 develop base:
  `3f6c7977550efaa3e03f9bd847480517e04f690b`
- calibration에서 제외한 독립 권역: `sangam`, `jungnang`, `cheonwang`, `cheonho`
- calibration 사용으로 confirmatory에서 제외한 권역: `gaehwa`, `hangnyeoul`, `isu`,
  `yeongnam`

고정한 12셀은 다음과 같다.

| 센터 | 07:00 | 13:00 | 18:00 |
|---|---|---|---|
| sangam | 2025-03-17 | 2025-06-17 | 2025-10-17 |
| jungnang | 2025-05-17 | 2025-09-17 | 2025-11-17 |
| cheonwang | 2025-10-17 | 2025-03-17 | 2025-06-17 |
| cheonho | 2025-11-17 | 2025-05-17 | 2025-09-17 |

단 한 번의 confirmatory 실행이 통과하려면 다음을 모두 만족해야 한다.

1. exact 12셀×3구간이며 모든 셀·구간의 신규 미충족 event가 0건이다.
2. 모든 셀·구간에서 미충족 요청 수와 품절 대여소-분이 각각 `no_rebalance`보다
   나빠지지 않는다.
3. 180분 aggregate 미충족 요청 수가 엄격히 감소한다.
4. 180분 aggregate 품절 대여소-분이 5% 이상 감소한다.
5. 180분 품절 시간이 엄격히 개선된 셀이 12개 중 8개 이상이다.
6. 모든 셀·구간의 pickup dispatch lag가 30분 이하다.
7. 모든 셀·구간에서 계획 이동량과 실행 이동량이 같고, 모든 경로가 cutoff 안에
   완료된다.

> **현재 상태:** manifest와 sidecar만 등록했다. Candidate lock 이후의 single-run
> claim과 12셀 실행은 아직 하지 않았으므로 confirmatory 통과·실패를 예단하지 않는다.

### Lock과 실행 계약

현 CLI는 `--repo-root`를 필수로 받고, resolve한 값이
`git rev-parse --show-toplevel`의 실제 repository root와 exact하게 같은지 검증한다.
재현 명령에서는 이를 절대경로로 넘긴다. worktree는 등록 branch의 clean commit이어야
하고, candidate HEAD와 등록 develop base가 모두 존재하며 base가 candidate의
ancestor여야 한다. Candidate lock에는 policy configuration뿐 아니라 route·urgency
scoring·backtest semantic version이 함께 들어간다.

Candidate lock과 결과 디렉터리는 resolve했을 때 repository 밖이어야 하며, 재현
명령에서는 명시적인 절대경로를 쓴다. CLI는 실제 import된 `evaluation`·`gold`·`core`·
`ml_core`와 center seed가 해당 candidate worktree의 파일인지 확인하고 12셀 뒤 Git
HEAD·clean 상태와 import binding을 다시 검증한다. `run`은 원천을 읽기 전에 exact
holdout 셀 집합 ID에 대응하는 private Git ref를 old-zero CAS로 만들고 claim JSON
blob을 고정한다. Raw 파일은 holdout·candidate·claim authority envelope에 결속하며,
검증이 끝난 exact 12개 raw SHA와 결과 digest는 별도 completion Git ref에 봉인한다.
따라서 lock이나 output 경로를 바꾸거나 같은 셀에 새 후보를 붙인 official CLI 재실행은
거부한다. 이 장치는 같은 repository 안의 우발적·CLI 재실행을 막는 로컬 guard이며,
권한 보유자의 ref 삭제나 별도 clone까지 막는 외부 append-only authority는 아니다.
다음은 경로 형태를 보여 주는 명령이며, 최종 clean candidate commit을 고정하기 전에는
실행하지 않는다.

```bash
cd /absolute/path/to/rebalance-policy-v3/loader

python -m evaluation.run_confirmatory_matrix lock \
  --repo-root /absolute/path/to/rebalance-policy-v3 \
  --candidate-lock /tmp/rebalance-confirmatory-v3/candidate-lock.json

python -m evaluation.run_confirmatory_matrix run \
  --repo-root /absolute/path/to/rebalance-policy-v3 \
  --candidate-lock /tmp/rebalance-confirmatory-v3/candidate-lock.json \
  --output-dir /tmp/rebalance-confirmatory-v3/result \
  --bootstrap-dir /absolute/path/to/data/issue163-full-year/bootstrap \
  --weather-csv /absolute/path/to/data/issue163-full-year/bootstrap/weather_realtime_2025.csv \
  --population-dir /absolute/path/to/data/issue163-full-year/population \
  --model-bundle /absolute/path/to/models/aws-temporary-model-2025-d20-h12-r20 \
  --center-seed /absolute/path/to/rebalance-policy-v3/docs/gold/dispatch-center-seed.yaml \
  --s3-endpoint http://localhost:9000 \
  --s3-bucket issue163-full-year
```

## 해석 가능한 주장과 한계

현재 calibration으로 직접 말할 수 있는 것은 다음이다.

> 고정된 관측 성공 수요와 명시한 모의 트럭 자원 아래에서 current route-v4 후보는
> 12개 calibration 셀의 60·120·180분 모두 신규 미충족 event를 만들지 않았고,
> 미충족 요청과 품절 시간을 악화시키지 않으면서 180분 aggregate 품절 시간을
> 7.773% 줄였다.

아직 다음 주장은 할 수 없다.

> 이 시스템이 실제 기존 운영보다 시민의 전체 잠재 수요를 일정 비율 더 충족한다.

이유는 다음과 같다.

1. 기록에는 현실에서 자전거가 없어 발생하지 않은 잠재 수요가 없다.
2. 기존 운영자의 route·truck·정확한 작업 시각 로그가 없다.
3. 고정 모델은 held-out 17일을 제외한 2025년 자료로 사후 학습됐으며 당시 실제 배포
   모델이 아니다.
4. 공통 이동 대수 상한은 적용하지만 실제 기존 운영과 차량 대수·차량 시간이 같았는지
   증명할 수 없다.
5. 현재 수치는 후보 선택에 사용한 calibration이며 독립 12셀 confirmatory는 아직
   실행하지 않았다.

결과의 `EvidenceGate`는 이 한계 때문에
`causal_superiority_vs_legacy_allowed=false`,
`publication_grade_system_claim_allowed=false`를 유지한다. Confirmatory를 통과해도
이는 고정 replay 계약에서의 채택 근거이며, 실제 인과 효과를 주장하려면 현장 작업
로그와 온라인 A/B 실험이 추가로 필요하다.
