"""풍속·풍향 -> 동서(UUU)·남북(VVV) 성분 분해 규칙.

기상청 실황 API는 `WSD`(풍속)·`VEC`(풍향)와 함께 `UUU`·`VVV`를 이미 계산해서 준다.
과거 CSV(ASOS 시간자료)에는 `UUU`·VVV`가 없고 풍속·풍향만 있어서, bootstrap이
같은 공식으로 채워 운영 수집분과 컬럼을 맞춘다.

**이 값은 발명이 아니라 가진 데이터의 무손실 변환이다.** 실황 API의 UUU/VVV와
대조 검증한 결과가 아래 TestMatchesLiveApi에 회귀로 박혀 있다.
"""

import math

import pytest

from core.wind import wind_components


class TestCardinalDirections:
    """기상학 관례: 풍향은 바람이 '불어오는' 방향이다. 북풍(0도)은 남쪽으로 부는
    바람이므로 남북 성분이 음수가 된다."""

    def test_north_wind_blows_southward(self):
        u, v = wind_components(10.0, 0.0)

        assert u == pytest.approx(0.0, abs=1e-9)
        assert v == pytest.approx(-10.0)

    def test_east_wind_blows_westward(self):
        u, v = wind_components(10.0, 90.0)

        assert u == pytest.approx(-10.0)
        assert v == pytest.approx(0.0, abs=1e-9)

    def test_south_wind_blows_northward(self):
        u, v = wind_components(10.0, 180.0)

        assert u == pytest.approx(0.0, abs=1e-9)
        assert v == pytest.approx(10.0)

    def test_west_wind_blows_eastward(self):
        u, v = wind_components(10.0, 270.0)

        assert u == pytest.approx(10.0)
        assert v == pytest.approx(0.0, abs=1e-9)

    def test_360_is_the_same_as_0(self):
        """ASOS의 풍향 고유값에 0과 360이 함께 나온다(둘 다 북풍)."""
        assert wind_components(3.0, 360.0) == pytest.approx(wind_components(3.0, 0.0))


class TestMagnitude:
    def test_magnitude_equals_wind_speed(self):
        """분해는 회전 변환이라 크기가 보존된다 — 어느 방향이든."""
        for direction in range(0, 360, 7):
            u, v = wind_components(4.2, float(direction))
            assert math.hypot(u, v) == pytest.approx(4.2)

    def test_calm_is_zero_in_both_components(self):
        assert wind_components(0.0, 250.0) == pytest.approx((0.0, 0.0))


class TestMatchesLiveApi:
    """2026-08-19 실황 API(getUltraSrtNcst)에서 격자 8곳의 WSD·VEC·UUU·VVV를 받아
    대조한 실측이다. 최대 절대오차는 UUU 0.10 / VVV 0.11 m/s였고, 그 오차는 API가
    주는 WSD·VEC가 이미 소수 첫째 자리로 반올림된 값이라는 것만으로 설명된다
    (예: VEC=274는 실제 273.5~274.4 중 하나다).

    공식이 틀리면 이 오차가 훨씬 커지므로, 0.15를 상한으로 박아 회귀를 막는다.
    """

    # (WSD, VEC, API의 UUU, API의 VVV)
    OBSERVED = [
        (1.6, 274.0, 1.6, 0.0),
        (1.4, 225.0, 1.0, 1.0),
        (1.4, 4.0, 0.0, -1.3),
        (1.5, 40.0, -0.9, -1.1),
        (0.6, 333.0, 0.3, -0.5),
        (1.9, 231.0, 1.5, 1.2),
        (1.0, 169.0, -0.1, 1.0),
        (0.6, 342.0, 0.2, -0.5),
    ]

    @pytest.mark.parametrize(("speed", "direction", "api_u", "api_v"), OBSERVED)
    def test_reproduces_api_components_within_rounding_error(self, speed, direction, api_u, api_v):
        u, v = wind_components(speed, direction)

        assert abs(u - api_u) <= 0.15, f"UUU 오차 {u - api_u:+.3f}"
        assert abs(v - api_v) <= 0.15, f"VVV 오차 {v - api_v:+.3f}"

    def test_sign_convention_matches_api_on_every_sample(self):
        """부호 관례가 뒤집혔는지는 크기 비교로 못 잡는다. API가 0.0을 준 성분은
        판정에서 빼고(부호가 없다) 나머지의 부호가 전부 일치해야 한다."""
        for speed, direction, api_u, api_v in self.OBSERVED:
            u, v = wind_components(speed, direction)
            if abs(api_u) >= 0.2:
                assert (u > 0) == (api_u > 0), f"UUU 부호 불일치: {u} vs {api_u}"
            if abs(api_v) >= 0.2:
                assert (v > 0) == (api_v > 0), f"VVV 부호 불일치: {v} vs {api_v}"


class TestFailures:
    def test_non_numeric_raises(self):
        with pytest.raises((TypeError, ValueError)):
            wind_components("바람", 0.0)

    def test_none_raises(self):
        with pytest.raises((TypeError, ValueError)):
            wind_components(None, 0.0)
