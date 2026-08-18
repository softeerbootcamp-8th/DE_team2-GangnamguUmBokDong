"""queries.py: _haversine_km 테스트 (#102, 대여소 주변 행사 검색에 쓰는 거리 계산)."""

from queries import _haversine_km


def test_same_point_is_zero_distance():
    assert _haversine_km(37.5, 127.0, 37.5, 127.0) == 0.0


def test_one_degree_latitude_matches_known_value():
    # 경도가 같을 때 위도 1도 차이는 지구 반지름(6371km) 기준 정확히 R*radians(1)이다.
    assert round(_haversine_km(37.0, 127.0, 38.0, 127.0), 6) == 111.194927


def test_symmetric_regardless_of_argument_order():
    a = _haversine_km(37.5, 127.0, 37.6, 127.1)
    b = _haversine_km(37.6, 127.1, 37.5, 127.0)
    assert a == b
