"""재배치 라우트 생성에 쓰이는 정책/튜닝값. urgency 계산 로직(urgency.py)과
분리해서 값만 바꿀 수 있게 둔다(core/scoring_config.py와 같은 패턴)."""

TRUCK_CAPACITY = 20  # 트럭 1대가 한 번에 실을 수 있는 최대 자전거 수
