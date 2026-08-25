"""Urgency 계산의 versioned 정책과 튜닝값을 정의한다.

실측 데이터 분포에 따라 조정될 값이라 계산 로직(rebalance/urgency.py,
core/forecast.py)과 분리한다. urgency_score는 배치(rebalance/)에서 계산하고
enrich_forecast_points는 실시간(apps/api)과 배치가 함께 쓰므로 이 상수들은
libs/core에서 공유한다.
"""

URGENCY_SCORING_CONFIG_VERSION = "urgency-scoring-v4-any-depletion"

# 현행 urgency reader가 anchor-25분부터 anchor-5분까지 읽는 과거 window의
# 시간 방향을 byte contract로 고정한다. 현재 anchor는 별도
# stock_publication_manifest가 소유하므로 전체 계산 window 수는 최대 6개다
# ("최대"인 이유는 아래 MIN_WINDOWS 주석 참고 — 실제로 쓴 수는 publication
# parameter의 stock_window_count에 그때그때 기록된다).
URGENCY_STOCK_HISTORY_OFFSETS_MINUTES = (-25, -20, -15, -10, -5)
URGENCY_STOCK_WINDOW_COUNT = len(URGENCY_STOCK_HISTORY_OFFSETS_MINUTES) + 1

# 위 5개 offset 전부를 필수로 요구하면 5분 tick 하나가 빠지는 순간 그 시각을
# 참조하는 25분 동안 urgency 게시가 전부 실패한다 — 지나간 실시간 스냅샷은
# 소급 수집이 불가능하므로 복구 수단도 없다(2026-08-22 운영 중 실측: scheduler가
# tick 2개를 건너뛰어 50분간 실패). 추세 계산(_trend_time_to_critical_v1)은
# 현재 anchor 1점을 포함해 2점부터 성립하므로 5개는 수학적 필요가 아니다.
# 그래서 "없는 window는 건너뛰되, 있는데 빠뜨리는 것은 금지"로 완화하고,
# 이 하한 미만이면 단발성 결측이 아닌 수집 장애로 보아 실패시킨다.
URGENCY_STOCK_HISTORY_MIN_WINDOWS = 2

RESPONSE_LAG_MIN = 30  # 트럭 출동~도착 소요시간
HALF_LIFE_MIN = 60  # 대응 여유시간이 이만큼 늘어날 때마다 시급성 점수가 절반이 됨
FIRST_FORECAST_MIN = 60  # 예측 데이터는 1시간 뒤부터만 존재(그 이전은 추세로 메꿈)

# 예측 재고가 정원의 이 비율 이하로 내려가면 supply_needed로 본다. 정확히
# 0(완전히 빔)만 잡으면 1~2대처럼 거의 다 빠진 상태가 "정상"으로 묻혀서, 대여
# 수요가 몰려도 어디에도 안 잡힌다(실측 2,746곳 중 445곳이 이 사각지대에
# 걸림, PR #64 리뷰). 10%~30% 구간에서 완만하게 늘어나 뚜렷한 변곡점은 없고,
# 20%는 hold_cnt=10 기준 2대 여유를 남기는 수준이라 이걸로 시작한다.
SUPPLY_LOW_STOCK_RATIO = 0.20

# 심각도(정원 대비 초과/부족량 비율 -> 0~1) 변환 곡선의 스케일. 비율이 이 값만큼
# 쌓일 때마다 남은 여유가 지수적으로 줄어든다(1 - e^(-ratio/SEVERITY_SCALE)).
# 실측 데이터(서울 전역 2,746곳)로 몇 가지 값을 대입해보고, 비율 1(정원만큼
# 초과/부족)이 40점대, 비율 4 이상(정원의 4배 이상)이 90점대로 나오는 이 값을
# 골랐다. 고정 배수에서 상한을 자르는 클램프 대신 점근 곡선을 쓴 이유는, 클램프를
# 쓰면 특정 배수를 넘는 순간부터 아무리 더 심해져도 점수가 그대로라 실측에서
# 회수필요 대여소의 35%가 똑같이 100점에 뭉쳐 있었기 때문이다(정원 2배 초과 =
# 클램프 상한). 점근 곡선은 상한이 없어서 극단치(실측 최대 22배)끼리도 계속
# 구분된다.
SEVERITY_SCALE = 1.5
