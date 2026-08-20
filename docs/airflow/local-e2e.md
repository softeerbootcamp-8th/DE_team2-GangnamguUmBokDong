# 로컬 Airflow E2E smoke

실제 운영 DAG인 `realtime_5min`이 Collector → Normalizer → Inference → Gold →
Urgency → Route와 rental replay까지 끝나는지 빈 로컬 환경에서 빠르게 확인하는
절차다. 별도 복제 DAG를 사용하지 않으며 운영 DAG의 pause·스케줄을 변경하지 않는다.

## 한 번에 실행

`.env`에 실제 API key를 준비하고 로컬 스택을 먼저 띄운다.

```bash
make up
make e2e-smoke
```

`make e2e-smoke`는 현재 KST 시각을 5분 단위로 내린 뒤 다음 작업을 수행한다.

1. 실제 `bike_station_realtime` collector를 한 번 실행해 직전 5분 station ID snapshot을 만든다.
2. 그 exact ID를 기준으로 로컬 전용 station/profile/model, nowcast, weather authority와
   직전 5개 재고 snapshot을 MinIO에 게시한다.
3. Dispatch center와 weather grid 기준 데이터를 로컬 PostGIS에 게시한다.
4. Paused `realtime_5min`을 Airflow `DAG.test()`의 보존되는 manual test run으로 실행한다.
5. 전체 태스크 상태 표와 성공/실패를 터미널에 출력한다.

`realtime_5min`이 unpaused이면 자동 스케줄과 Gold writer가 겹칠 수 있으므로 smoke는
실행 전에 중단한다. Test run은 scheduler의 pause 상태를 건드리지 않으며 Airflow
metadata와 task log를 UI에 남긴다.

Airflow UI는 `.env`의 `AIRFLOW_WEBSERVER_PORT`로 접속한다. `.env.example`의
기본 설정을 사용하면 `http://localhost:8081`이다.
사용자는 `admin`, 비밀번호는 로컬 실행 중 생성되는
`airflow/simple_auth_manager_passwords.json.generated`에서 확인한다. UI에서
`realtime_5min` DAG를 열면 test run의 태스크가 Grid 화면에서 순서대로 초록색이 된다.

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

Fixture는 로컬 Compose의 MinIO/PostGIS에만 쓴다. `stationId`는 숫자 대여소 번호를
추측하지 않고 먼저 수집한 직전 exact realtime snapshot에서 가져오며, 나머지 fixture는
그 snapshot을 결정적으로 변환한다. 현재 window는 미리 채우지 않으므로 운영 DAG의
`collect_bike_station_realtime`도 실제 수집 경로를 실행한다. 이미 authoritative
snapshot이 있는 과거 window는 덮어쓰지 않고 재사용한다.

## 실패 확인

터미널의 task 상태 표에서 `failed` 태스크를 찾고 Airflow Grid에서 해당 태스크의
로그를 연다. Fixture 자체가 부족하면 DAG test 전에 `seed` 또는 `check` 단계가
nonzero로 종료되어 원인을 출력한다.
