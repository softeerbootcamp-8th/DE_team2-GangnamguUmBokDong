# 로컬 Airflow E2E smoke

`e2e_realtime`의 13개 태스크가 Collector → Normalizer → Inference → Gold →
Urgency → Route 순서로 끝까지 이어지는지 빈 로컬 환경에서 빠르게 확인하는 절차다.
이 경로는 운영 데이터·운영 모델·운영 DAG 스케줄을 변경하지 않는다.

## 한 번에 실행

`.env`에 실제 API key를 준비하고 로컬 스택을 먼저 띄운다.

```bash
make up
make e2e-smoke
```

`make e2e-smoke`는 현재 KST 시각을 5분 단위로 내린 뒤 다음 작업을 수행한다.

1. 로컬 전용 nowcast, 보강 station master, 직전 5개 재고 snapshot을 MinIO에 게시한다.
2. 실제 immutable model snapshot/serving release 계약으로 작은 LightGBM 모델 포인터를 만든다.
3. Dispatch center와 weather grid 기준 데이터를 로컬 PostGIS에 게시한다.
4. 같은 window로 `e2e_realtime`을 trigger하고 terminal state까지 기다린다.
5. 13개 태스크 상태 표와 성공/실패를 터미널에 출력한다.

Airflow UI는 `.env`의 `AIRFLOW_WEBSERVER_PORT`로 접속한다. `.env.example`의
기본 설정을 사용하면 `http://localhost:8081`이다.
사용자는 `admin`, 비밀번호는 로컬 실행 중 생성되는
`airflow/simple_auth_manager_passwords.json.generated`에서 확인한다. UI에서
`e2e_realtime` DAG를 열면 실행 중인 태스크가 Grid 화면에서 순서대로 초록색이 된다.

## Fixture만 준비하거나 확인

기본 logical time은 현재 KST 5분 경계다.

```bash
make seed-e2e
make e2e-preflight
```

특정 window를 재현하려면 두 명령에 같은 값을 넘긴다.

```bash
make seed-e2e E2E_LOGICAL_DTTM=2026-08-20T16:40:00+09:00
make e2e-preflight E2E_LOGICAL_DTTM=2026-08-20T16:40:00+09:00
```

Fixture는 로컬 Compose의 MinIO/PostGIS에만 쓰이며 같은 입력으로 재실행할 수 있다.
실제 Collector snapshot이 이미 있는 과거 window는 덮어쓰지 않고 그 authoritative
snapshot을 재사용한다.

## 실패 확인

터미널의 task 상태 표에서 `failed` 태스크를 찾고 Airflow Grid에서 해당 태스크의
로그를 연다. Fixture 자체가 부족하면 DAG trigger 전에 `seed` 또는 `check` 단계가
nonzero로 종료되어 원인을 출력한다.
