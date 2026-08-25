# 기존 모델 inference runtime 경량화 (2026-08-25)

## 범위

이번 변경은 기존 champion 모델과 serving release를 그대로 사용하면서 5분 inference의
fallback profile 메모리와 생활인구 조회량만 줄인다. 학습 코드, feature 순서·dtype,
LightGBM artifact, 예측 수식, 결과 스키마, archive와 serving pointer는 변경하지 않는다.

- station profile은 실제로 읽은 `(month, dow)` 조합별 배열만 만든다.
- fallback에서 참조하지 않는 `rental_std`와 `return_std`는 Parquet에서 읽거나
  runtime 배열에 보관하지 않는다. 원본 profile artifact의 스키마와 파일은 유지한다.
- 생활인구는 최근 1시간의 13개 5분 Parquet를 모두 읽은 뒤 하나를 고르지 않고,
  최신 key부터 읽어 첫 유효 tick에서 멈춘다.

Q/X/Y 모델, full-year probe, policy replay 확대, 1h lag snapshot, quantile 변경,
정적 피처, 재학습과 EMR 검증은 이 변경에 포함하지 않는다.

## 출력 동일성

현재 serving release의 station profile artifact
`sha256=55a5afedb7c956fcdfed40750147cfbaf214e344b943c5f8c37e5e29050f1a19`
에서 2025년 6월 화요일 partition 194,904행을 읽어 두 구현의 `rental_mean`과
`return_mean` lookup 결과를 원본 행 순서로 복원했다. 두 결과의 SHA-256은 모두
다음과 같았으며 NaN 위치를 포함한 값 비교도 일치했다.

```text
897c5fabd2ccb40f79687c66c61747a105c69aae5f03ddc9ae671426188ba0ea
```

생활인구는 기존과 같이 `[T-60분, T]`에서 가장 최근의 비어 있지 않은 normalized
tick을 선택한다. 정확한 T가 있으면 GET은 `13회 → 1회`, 바로 전 tick만 있으면
`13회 → 2회`, 모두 없으면 기존과 같은 최대 13회다. 캐시 key와 fallback 결과는
바뀌지 않는다.

## 격리 실측

같은 WSL, Python 환경과 위 실제 artifact를 사용했다. OS page cache를 데운 뒤 각
구현을 새 프로세스에서 한 번씩 실행했다. 아래 wall은 전체 5분 DAG가 아니라
station profile read·배열 구성·전체 행 lookup 검증 구간이다.

| 항목 | 기존 `develop` | 변경 | 차이 |
|---|---:|---:|---:|
| 선택 Parquet 컬럼 | 8 | 6 | std 2개 제외 |
| 선택 컬럼 compressed bytes 합 | 70,901,285B | 33,223,898B | -53.1% |
| 필터 후 DataFrame 메모리 | 4,288,020B | 2,728,788B | -36.4% |
| runtime profile 배열 | 261,950,976B | 1,559,232B | -99.4% |
| 배열 구성 | 0.2325초 | 0.0249초 | -89.3% |
| 검증 구간 wall | 0.3524초 | 0.0997초 | -71.7% |
| 프로세스 peak RSS | 643.5MiB | 424.3MiB | -34.1% |

Parquet compressed bytes는 실제 파일 metadata에서 선택 컬럼 chunk의 크기를 합산한
값이다. 네트워크 요청의 HTTP framing이나 S3 SDK buffer는 포함하지 않는다. RSS는
Python import와 Parquet decode까지 포함하며, 전체 inference 동시 실행의 peak를
뜻하지 않는다.

## 검증

```bash
cd ml
python -m pytest -q inference/tests
```

결과는 `128 passed, 1 skipped`다. 테스트는 profile 값·달력 경계·model grid 내림,
중복/잘못된 key fail-closed, authority pinned profile, 생활인구 최신 tick·이전 tick
fallback·캐시, 사용 컬럼 projection을 포함한다.
