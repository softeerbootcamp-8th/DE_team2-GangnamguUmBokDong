"""대여소를 11개 지역센터 중 하나에 근사 배정한다.

지역센터의 정확한 관할 경계(어느 구·동까지 담당하는지)는 공개 자료로 확인되지
않는다. 그래서 각 대여소를 좌표상 가장 가까운 지역센터에 배정하는 최근접
(nearest-neighbor) 근사를 쓴다 — 실제 관할과 다를 수 있으며(특히 두 지역센터
사이 경계 근처), 대여소를 현장 단위로 의미 있게 묶어서 보여주는 용도로만 쓴다.

좌표는 서울시설공단 자료의 주소를 기준으로 조사한 값이다. 대부분 정확한 건물
지점이 아니라 인근 역·공원 등 랜드마크 좌표로 대체했고(지역센터끼리 몇 km씩
떨어져 있어 최근접 배정 용도로는 충분한 정밀도), 영남만 정확한 위치를 찾지
못해 행정동 중심점을 썼다(최대 1km 오차 가능).
"""

import math

DISPATCH_CENTERS: list[tuple[str, float, float]] = [
    ("상암", 37.5683, 126.8972),
    ("중랑", 37.5583, 127.0581),
    ("세종로", 37.5716, 126.9769),
    ("도봉", 37.68944, 127.04583),
    ("훈련원", 37.5674, 127.0031),
    ("이수", 37.4982, 126.9845),
    ("개화", 37.5780, 126.7980),
    ("천왕", 37.48667, 126.83861),
    ("영남", 37.5178, 126.9037),  # 근사치 중에서도 특히 부정확함(행정동 중심점)
    ("천호", 37.5385167, 127.1237667),
    ("학여울", 37.49352, 127.07954),
]


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """두 좌표 사이의 거리를 구면 거리(km)로 계산한다."""
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def nearest_region(lat: float, lon: float) -> str:
    """대여소 좌표에서 가장 가까운 지역센터의 이름을 반환한다."""
    return min(
        DISPATCH_CENTERS,
        key=lambda center: _haversine_km(lat, lon, center[1], center[2]),
    )[0]
