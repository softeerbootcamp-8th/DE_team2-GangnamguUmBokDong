# 재배치 시스템 point-in-time 백테스트

> **문서 상태:** 현재 평가 계약은 `point-in-time-policy-backtest-v3`, 운영 경로
> 알고리즘은 `route-v3-supply-led`, 긴급도 계산 설정은 `urgency-scoring-v4-any-depletion`이다. 백테스트
> 구현은 `loader/evaluation/`에 있으며 계약·시뮬레이터 테스트로 검증한다. 원천 CSV,
> 모델 bundle, MinIO station master와 생성 결과는 Git에 포함하지 않는다.

이 평가는 과거 실측 재고에서 출발해 당시까지 관측 가능한 입력으로 대여·반납 모델을
5분마다 다시 실행하고, 시민 대여 요청과 트럭 작업을 사건 순서대로 재생한다. 미래
실제 수요로 수량을 만드는 oracle 실험은 경로 디버깅 용도일 뿐 시스템 성능 근거로
사용하지 않는다.

현재 production 후보는 다음 정책이다.

```text
risk_h2_r20_z0000_f008_cd060_s5
```

- 예측 보호 구간: 2시간
- 최소 재고: 정원의 20%
- 회수 불확실성 계수: 0
- 한 판단의 회수 상한: 현재 재고의 8%
- 동일 공급원 동시 분할 금지
- 동일 공급원 재회수 cooldown: 배차 후 60분
- 한 경로의 최대 대여소: 5곳
- 현장 도착 시 별도의 안전재고·비율 재계산 없음

마지막 항목이 중요하다. 최종 정책은 시뮬레이터에만 존재하는 도착 시점 clamp에
의존하지 않는다. 계획 단계에서 산출한 수량 자체가 production 작업 지시이며, 실행
시뮬레이션은 실제 재고와 트럭 적재량이라는 물리 한계만 적용한다.

## 평가 질문

동일한 시작 재고와 실제 관측 수요에서 다음 상태를 비교한다.

1. `no_rebalance`: 시민 대여·반납만 재생한다.
2. `추정 기존 운영`: 시간별 실측 재고에서 시민 흐름을 제거한 운영 잔차를 적용한다.
3. `legacy_s5`: 기존 수량·공급원 보호 동작을 최대 5곳 경로로 재생하는 비교 정책이다.
4. `risk_h2_r20_z0000_f008_cd060_s5`: 최종 route-v3 production 후보다.

`legacy_s5`와 개선 후보도 결과 provenance에는 동일한 `route-v3-supply-led` 실행 엔진으로
기록된다. 차이는 versioned policy configuration이며, 과거 알고리즘 이름을 현재
production provenance로 사용하지 않는다.

재배치가 없는 별도의 사실 데이터가 존재하는 것은 아니다. `no_rebalance`와 모델
정책은 같은 관측 성공 수요를 사용하는 반사실 replay다. 대여가 실패하면 그 이용의
실제 반납도 제거한다.

기존 운영자의 station별 순개입은 각 정시 구간에서 다음 항등식으로 역산한다.

```text
운영 잔차 = 다음 실측 재고 - (현재 실측 재고 - 시민 대여 + 시민 반납)
```

공개 데이터로는 개입의 정확한 시각·경로·트럭을 알 수 없다. 따라서 잔차를 각 시간
구간의 초·중·말에 적용한 세 결과를 모두 기록하고, 세 경우 모두 구간 말 실측 재고를
오차 0으로 복원하는지 검증한다.

## 평가 계약 v3

기본 근거 실행은 다음 조건을 고정한다.

- 운영 판단 주기: 5분
- 평가 구간: 60분·120분·180분을 한 묶음으로 실행
- 트럭: 권역당 3대, 용량 20대
- 주행: 20km/h, 대여소 작업 3분
- 승인 지연: 자동 정책 평가를 위해 0분
- 배차 cutoff: 종료 전에 마지막 작업과 센터 복귀가 가능한 경로만 시작
- production 최대 대여소: 경로당 5곳
- 공통 이동 예산: 같은 구간의 기존 운영 잔차 중 `min(유입, 유출)`

120분은 모델 예측 범위가 아니라 현장 작업 단위를 검토하기 위해 사전에 정한 구간이다.
특정 결과가 좋아 보이는 길이를 사후 선택하지 않도록 기본 CLI는 60·120·180분을 모두
요구한다.

후보 교정과 독립 검증은 원인을 빠르게 분리하기 위해 180분만 실행할 수 있다. 그러나
180분 단독 결과는 전체 기본 근거 묶음을 대신하지 않는다. 이 문서에 기록한 최종 8%
후보의 새 raw 결과도 현재 180분만 존재하므로, 60분·120분 성능은 아직 결론내리지
않는다.

계약 v3 결과는 날짜별로 다음을 함께 검증·기록한다.

- `backtest_contract_version=point-in-time-policy-backtest-v3`
- `route_algorithm_version=route-v3-supply-led`
- `urgency_scoring_config_version=urgency-scoring-v4-any-depletion`
- 원천 대여·재고·날씨·생활인구 파일의 경로·크기·SHA-256
- station master와 station crosswalk의 SHA-256
- 날짜 외 전체 운영 계약과 policy configuration의 동일성
- point-in-time 입력, 5분 tick, 이동 예산, 센터 복귀 cutoff

진행 중 경로의 전체 계획량은 coverage로 차감한다. 작업이 끝난 트럭도 센터에
돌아오기 전까지 새 경로를 받을 수 없고, 이전 tick의 미승인 proposed 경로는 다음
계산으로 교체된다.

채택 gate는 정책별로 다음 조건을 요구한다.

1. 모든 평가 날짜·구간에서 미충족 대여가 `no_rebalance`보다 늘지 않는다.
2. 모든 평가 날짜·구간에서 품절 대여소-분이 늘지 않는다.
3. 180분 합산 미충족 대여는 엄격히 감소한다.
4. 평가한 각 구간의 합산 품절 대여소-분은 엄격히 감소한다.

production 채택 근거로 사용할 때는 이 gate를 기본 60·120·180분 전체 묶음에 적용한다.

## 최종 수량·경로 정책

기존 urgency의 점수와 `supply_needed`/`retrieval_needed` 판정은 유지하되, route가
소비하는 이동 수량을 위험 구간 정책으로 계산한다. 현재 재고를 `current`, 정원을
`capacity`, 향후 두 시간 평균 재고 경로를 `stock_path`라 하면:

```text
minimum_stock = ceil(capacity * 0.20)

pickup_qty = min(
    current,
    floor(max(stock_path) - capacity),
    floor(min(stock_path) - minimum_stock),
    floor(current * 0.08),
)
```

각 항은 음수가 되지 않도록 0에서 자른다. 즉 정원 초과분을 회수하더라도 향후 두 시간
평균 경로의 최저점에서 최소 재고를 지키고, 한 번에 현재 재고의 8%보다 많이 계획하지
않는다.

공급량은 평균 경로의 최저 재고를 최소 재고까지 높이되 현재 빈 거치 공간을 넘지
않는다.

```text
dropoff_qty = min(
    ceil(minimum_stock - min(stock_path)),
    capacity - current,
)
```

경로 계획은 다음 공급원 보호를 추가한다.

1. 센터별 최고 supply urgency가 경로 ordinal을 소유한다.
2. 안전한 pickup은 `center→pickup→최고 supply` 총거리 순으로 선택한다.
3. 모든 pickup 뒤 최고 supply를 첫 dropoff로 방문한다.
4. 같은 pickup 대여소를 한 tick의 여러 경로에 나누지 않는다.
5. 진행 중 경로가 pickup을 예약한 대여소는 새 회수 후보에서 제외한다.
6. 배차 후 cooldown 동안 같은 대여소를 다시 회수하지 않는다.
7. pickup·dropoff 합계가 같고 트럭 용량 20대 이하인 완결 작업만 만든다.
8. 한 경로는 최대 5개 대여소를 방문한다.

정책 값과 cooldown 대여소 집합은 publication fingerprint의 immutable 입력으로 남긴다.

## 정보시점과 누출 방지

- 시작 재고: 평가 시작 정시의 실측값만 사용하며 이후에는 정책별 사건 재생 상태만
  사용한다.
- 대여소 식별자: 월 대여 이력의 공공 번호와 내부 `ST-<suffix>` 사이 1:1 crosswalk를
  만들고, 다대일·일대다 대응은 fail-closed한다.
- 대여 lag: `[T-100분, T-40분)`에 시작하고 T까지 반납되어 관측 가능한 성공 이용만
  센다.
- 반납 lag: `[T-60분, T)`의 성공 반납만 센다.
- 날씨: anchor보다 60분 이전인 최신 관측만 사용하고 관측시각과 cutoff를 기록한다.
- 생활인구: 대상일보다 1~4주 전 자료의 0.4/0.3/0.2/0.1 가중평균을 사용한다. 모두
  결측인 셀만 5~8주 전 최근값으로 보완하고, 끝까지 결측인 격자는 평가에서 제외한다.
- 모델: `aws-temporary-model-2025-d20-h12-r20`의 원본 bytes를 SHA-256으로 고정한다.
  매월 17일은 학습에서 제외된 test split이다.

## 실행

기본 60·120·180분 근거 실행은 다음 형태다.

```bash
docker compose -f ops/compose/docker-compose.yml exec -T \
  -w /workspace/loader airflow-scheduler \
  env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/loader \
  uv run --frozen python -m evaluation.run_policy_backtest \
  --date 2025-06-17 \
  --center hangnyeoul \
  --start-hour 6 \
  --evaluation-minutes 60 120 180 \
  --fleet-size 3 \
  --max-stops 5 \
  --rental-csv '../data/issue163-full-year/bootstrap/서울특별시 공공자전거 대여이력 정보_2506.csv' \
  --stock-csv '../data/issue163-full-year/bootstrap/대여소별 공공자전거 대여가능 수량_2506.csv' \
  --s3-endpoint http://minio:9000 \
  --s3-bucket issue163-full-year
```

아래 정책 선택 결과는 2025-06-17·2025-10-17 두 날짜, 180분에서 회수 상한만
5%·8%·10%·12%·15%로 바꿔 생성했다.

```bash
python -m evaluation.run_policy_search \
  --dates 2025-06-17 2025-10-17 \
  --center hangnyeoul \
  --start-hour 6 \
  --evaluation-minutes 180 \
  --fleet-size 3 \
  --protection-hours 2 \
  --minimum-stock-ratios 0.2 \
  --uncertainty-z 0 \
  --max-pickup-stock-fractions 0.05 0.08 0.10 0.12 0.15 \
  --pickup-cooldown-minutes 60 \
  --max-stops 5 \
  --include-legacy \
  --output-dir /tmp/rebalance-v3-fraction-search-20260825
```

선택에 쓰지 않은 8개 날짜의 독립 검증은 8%를 고정한 뒤 실행했다.

```bash
python -m evaluation.run_policy_search \
  --dates 2025-03-17 2025-04-17 2025-05-17 2025-07-17 \
          2025-08-17 2025-09-17 2025-11-17 2025-12-17 \
  --center hangnyeoul \
  --start-hour 6 \
  --evaluation-minutes 180 \
  --fleet-size 3 \
  --protection-hours 2 \
  --minimum-stock-ratios 0.2 \
  --uncertainty-z 0 \
  --max-pickup-stock-fractions 0.08 \
  --pickup-cooldown-minutes 60 \
  --max-stops 5 \
  --output-dir /tmp/rebalance-v3-independent-f008-20260825
```

## 8% 상한 선택 결과

교정 집합은 과회수 반례가 드러난 2025-06-17과 2025-10-17 두 날짜다. 다른 조건은
고정하고 회수 상한만 비교했다.

| 회수 상한 | 관측 요청 | 미충족 변화 | 품절 대여소-분 변화 | 날짜별 충족률 개선/동일/악화 | 이동 | 차량 분 |
|---:|---:|---:|---:|---:|---:|---:|
| 5% | 4,987 | -1 | -5.96% | 1/1/0 | 58 | 911.2 |
| **8%** | **4,987** | **-3** | **-8.56%** | **2/0/0** | **63** | **871.1** |
| 10% | 4,987 | -2 | -9.53% | 1/0/1 | 73 | 945.7 |
| 12% | 4,987 | -2 | -9.05% | 1/1/0 | 74 | 912.0 |
| 15% | 4,987 | -1 | -9.73% | 1/0/1 | 73 | 885.3 |

`미충족 변화`와 `품절 대여소-분 변화`는 `no_rebalance` 대비다. 10%와 15%는 합산
지표가 좋아도 두 날짜 중 한 날짜의 충족률을 악화시켰다. 8%는 두 날짜 모두 충족률을
개선했고 합산 미충족 감소도 3건으로 가장 컸다. 시민 수요 충족률을 primary metric으로
두므로 8%를 선택하고, 그 뒤 독립 검증이 끝날 때까지 값을 바꾸지 않았다.

교정 두 날짜의 8% 결과는 다음과 같다.

| 날짜 | 기준 미충족 | 8% 미충족 | 품절 대여소-분 변화 | 이동 | 경로 | 차량 분 |
|---:|---:|---:|---:|---:|---:|---:|
| 2025-06-17 | 12 | 10 | -290.9 | 30 | 9 | 435.3 |
| 2025-10-17 | 12 | 11 | -459.6 | 33 | 9 | 435.8 |

## 독립 8개 날짜 검증

선택에 사용하지 않은 2025-03·04·05·07·08·09·11·12월 17일을 같은 학여울 06:00,
180분 계약으로 실행했다. 모델 bundle SHA-256은
`c677e8e192caef85adc7293a26019ea28681199c75bc085ae86702d300bb0afb`로 같았다.

| 정책 | 관측 요청 | 충족률 | 미충족 | 미충족 변화 | 품절 대여소-분 | 변화 | 이동 | 경로 | 차량 분 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `no_rebalance` | 8,382 | 99.5824% | 35 | 기준 | 43,827.7 | 기준 | 0 | 0 | 0.0 |
| 8% route-v3 | 8,382 | 99.7375% | 22 | -13 | 39,429.5 | -10.04% | 262 | 65 | 3,088.3 |

날짜별 충족률은 4개 날짜에서 개선되고 4개 날짜에서 같았으며 악화는 0개였다. 품절
대여소-분도 8개 날짜 모두 감소했다. 따라서 180분 독립 검증에서는 시민 서비스
비악화와 품절 시간 감소가 함께 재현됐다.

이 결과는 최종 8% 정책의 180분 근거다. 아직 같은 정책으로 60분·120분 전체 날짜를
실행한 raw 결과가 없으므로 해당 구간의 변화율, 미충족 건수 또는 전체
60·120·180분 acceptance 통과를 주장하지 않는다.

## 해석 가능한 주장과 불가능한 주장

이 평가로 직접 말할 수 있는 것은 다음뿐이다.

> 고정된 관측 수요와 명시한 모의 트럭 자원 아래에서 8% route-v3 정책은 교정 2일과
> 독립 8일의 180분 replay 모두 `no_rebalance`보다 미충족 대여를 늘리지 않았고,
> 독립 8일 합산 미충족과 품절 대여소-분을 함께 줄였다.

아직 다음 주장은 할 수 없다.

> 우리 시스템이 실제 기존 운영보다 시민의 전체 잠재 수요를 일정 비율 더 충족한다.

이유는 다음과 같다.

1. 기록에는 현실에서 자전거가 없어 시도하지 못한 잠재 수요가 없다.
2. 기존 운영자의 route·truck·정확한 작업시각 로그가 없다.
3. 고정 모델은 2025년 전체의 다른 날짜로 사후 학습돼 당시 실제 배포 모델이 아니다.
4. 같은 이동 대수 상한은 강제했지만 기존 운영과 같은 차량 대수·차량시간인지는
   증명할 수 없다.
5. 최종 8% 정책의 새 독립 결과는 현재 학여울 06:00·180분에 한정된다.

결과의 `EvidenceGate`는 위 한계 때문에
`causal_superiority_vs_legacy_allowed=false`,
`publication_grade_system_claim_allowed=false`로 유지한다. production 채택 전에는
60·120·180분 전체 묶음, 다른 권역·시간대 반복 검증과 현장 작업 로그 또는 A/B
실험이 추가로 필요하다.
