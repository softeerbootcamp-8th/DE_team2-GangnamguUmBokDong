"""구조화 로그 설정 — 고정 필드(source_id·window·attempt) 주입.

구현 예정: docs/collector/implementation-issues.md #8
설계 근거: docs/collector/implementation-plan.md 9절 (로깅)

## 구현할 것

- `source_id` · `window` · `attempt`를 **모든 로그 레코드에 자동으로 붙인다.**
  `logging.LoggerAdapter`나 `logging.Filter` 중 하나로 구현하고, 호출부가 매번 같은
  필드를 다시 적지 않게 한다.
- 출력은 컨테이너 stdout으로 보낸다. 형식은 `key=value` 평문에서 시작한다
  (JSON으로 바꿀지는 수집기를 붙이는 시점에 결정한다).
- 레벨 규칙 — 정상 단계는 INFO, `PARTIAL`은 WARN, `FAILED`는 ERROR.

## 출력량 규칙: 배치당 몇 줄, 행당 0줄

2,765행 × 288회/일이면 행 단위 로그는 로그를 터뜨린다. **행 상세는 quarantine 파일이
담당한다.** 조각마다, 그리고 라운드마다 로그를 남기지도 않는다. 아래 3줄이 한 배치의
정상 출력 전부다.

    INFO  source_id=bike_station_realtime window=2026-08-12T14:10Z
          stage=bronze_written parts=3/3 rounds=1 rows=2765 bytes=482113 ms=1203
    WARN  source_id=… stage=validated status=PARTIAL kept=2740 repaired=31
          dropped=25 drop_ratio=0.009 completeness=0.991
    INFO  source_id=… stage=completed revision=1 key=s3://…/1410.parquet

`stage` 값은 manifest의 `Stage`와 같은 어휘를 쓴다. `fetched`는 없다 — 조각을 도착 즉시
저장하므로 fetch 완료와 bronze 완료가 같은 시점이고, 위 첫 줄이 그 둘을 한꺼번에 알린다.
`parts`는 **받은 조각 / 계획한 조각**이고 `rounds`는 라운드 수다. 라운드별 로그 대신
이 두 값으로 재시도가 있었는지 알 수 있다.

누락이 발생하면 첫 줄이 WARN이 되고 무엇이 빠졌는지 붙는다.

    WARN  source_id=… stage=bronze_written parts=2/3 rounds=3
          missing=page-02001-02765 missing_rows=765 completeness=0.717

실패 시에는 `failure_reason`을 함께 남긴다. 수집 게이트와 폐기 게이트는 사유가 다르므로
로그만 봐도 재시도할 실패인지 config를 고칠 실패인지 구분된다.

    ERROR source_id=… stage=validated status=FAILED failure_reason=quality_gate
          dropped=412 drop_ratio=0.149
    ERROR source_id=… stage=bronze_written status=FAILED failure_reason=fetch_error
          missing_ratio=0.638 reason=circuit_breaker

백필 실행은 `revision` 변화를 남긴다. 이 한 줄이 "silver 내용이 언제 바뀌었는지"의
유일한 기록이므로 하류 재처리를 추적할 때 쓰인다.

    INFO  source_id=… mode=backfill parts=1 filled=page-02001-02765
          revision=1→2 completeness=0.717→1.0

## 주의

- 인증키가 로그에 남지 않게 한다. 서울 API는 키를 URL 경로에 담으므로 예외 메시지에
  URL이 실려 나가는 경로를 특히 조심한다. 마스킹을 여기서 할지 어댑터에서 할지는
  #8에서 한 곳으로 정한다.
- boto3 · httpx의 기본 로거가 시끄러우면 레벨을 따로 낮춘다.
"""
