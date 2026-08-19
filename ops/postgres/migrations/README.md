# 운영 RDS 마이그레이션

로컬 Compose는 `postgres-schema-init`가 초기화 스크립트를 다시 실행하지만, 운영
RDS에는 같은 실행 경로가 없다. 스키마와 애플리케이션 코드가 함께 바뀌는 경우 이
디렉터리의 전용 SQL을 배포 순서에 맞춰 별도로 실행한다.

## station_stock 최신 1건 전환

기존 loader는 `(sta_id, observed_at)`, 새 loader는 `sta_id`를 충돌 키로 사용한다.
DB와 loader 버전이 어긋나면 어느 한쪽의 `ON CONFLICT`가 실패하므로 다음 순서를
지킨다.

1. Airflow 스케줄러 또는 `loader` 적재 작업을 중지한다.
2. 필요하면 `station_stock`을 백업한다. 제거되는 관측 이력의 원본은 S3 Silver에도
   보존되어 있다.
3. 운영 RDS에 전용 마이그레이션을 실행한다.

   ```bash
   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
     -f ops/postgres/migrations/20260819_station_stock_latest.sql
   ```

4. PK와 대여소별 행 수를 확인한다.

   ```sql
   SELECT pg_get_constraintdef(oid)
     FROM pg_constraint
    WHERE conrelid = 'station_stock'::regclass
      AND contype = 'p';

   SELECT sta_id, count(*)
     FROM station_stock
    GROUP BY sta_id
   HAVING count(*) > 1;
   ```

   첫 쿼리는 `PRIMARY KEY (sta_id)`, 두 번째 쿼리는 0행이어야 한다.

5. `conflict_cols: [sta_id]`가 적용된 새 loader를 배포한다.
6. loader 적재를 한 번 실행해 성공을 확인한 뒤 스케줄을 재개한다.
7. 새 API를 배포한다.

`002_gold_schema.sh` 전체에는 다른 Gold 테이블의 초기화 구문도 있으므로 운영 RDS의
이 변경을 위해 직접 실행하지 않는다.

