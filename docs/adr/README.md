# Architecture Decision Records

ADR은 중요한 기술 선택의 배경, 결정과 결과를 기록한다. 이미 내려진 결정은 구현이 바뀌어도 삭제하거나 현재 내용으로 덮어쓰지 않고, 후속 ADR에서 대체 관계를 남긴다.

## 상태

| 상태 | 의미 |
| --- | --- |
| `제안` | 검토 중이며 아직 시스템의 기준이 아니다. |
| `채택` | 현재 시스템에 적용되는 유효한 결정이다. |
| `대체됨` | 후속 ADR이 이 결정을 대신한다. |
| `폐기` | 채택하지 않았거나 더 이상 적용하지 않는다. |

## 목록

| ADR | 결정일 | 현재 상태 | 비고 |
| --- | --- | --- | --- |
| [0001: Collector 모듈 설계](0001-collector-module-design.md) | 2026-08-12 | 채택 | 부분 수집·백필은 ADR-0004가 확장 |
| [0002: 초기 Gold 스키마 설계](0002-gold-schema-design.md) | 2026-08-13 | 대체됨 | ADR-0006이 전체 결정 대체 |
| [0003: Bronze streaming과 확장 경계](0003-bronze-streaming-and-scaling-boundaries.md) | 2026-08-12 | 채택 | 요청 key·부분 실패 처리는 ADR-0004가 확장 |
| [0004: 부분 수집과 backfill](0004-partial-fetch-and-backfill.md) | 2026-08-13 | 채택 | Adapter·Pipeline·manifest 테스트 구현 확인 |
| [0005: LightGBM 분산 학습 준비](0005-lightgbm-distributed-training.md) | 2026-08-14 | 대체됨 | 다중 머신 경로가 구현되지 않아 ADR-0007이 대체 |
| [0006: Gold PostGIS 서빙 모델](0006-gold-postgis-schema.md) | 2026-08-19 | 채택 | DDL·Loader·API·통합 테스트 구현 확인 |
| [0007: 단일 머신 LightGBM 학습](0007-single-machine-lightgbm-training.md) | 2026-08-24 | 채택 | 단일 EC2·lazy dataset·checkpoint 구현 확인 |

## 작성 규칙

1. 결정 하나당 ADR 하나를 작성한다.
2. 채택된 ADR의 본문은 사후에 현재 구현 설명으로 바꾸지 않는다.
3. 결정이 달라지면 기존 ADR을 `대체됨`으로 표시하고 새 ADR을 추가한다.
4. 배경에는 문제와 제약을, 결정에는 선택한 원칙을, 결과에는 장점과 비용을 적는다.
5. 구현 파일은 참고 링크일 뿐 결정 자체를 대신하지 않는다.
6. 새 문서는 [template.md](template.md)를 복사해 작성한다.
