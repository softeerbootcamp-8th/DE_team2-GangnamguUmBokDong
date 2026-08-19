"""대여소 매핑표 생성 테스트: 3단계 폴백·번호 정규화·통계.

재고 CSV는 `대여소번호`(`00102`)를 주는데 collector 스키마는 `stationId`(`ST-4`)를
required로 요구한다. 세 소스를 겹쳐 매핑표를 만든다 — 실측 커버리지는 API 단독
97.1%, 대여이력 CSV union 99.32%다(대여소 2,799 기준).
"""

import json
import logging

import httpx
import pytest

from bootstrap.station_join import build

HISTORY_HEADER = (
    "자전거번호,대여일시,대여 대여소번호,대여 대여소명,반납대여소번호,반납대여소명,"
    "대여대여소ID,반납대여소ID\n"
)


def _station(station_id, name, rack="15", shared="13", lat="37.55", lon="126.91"):
    return {
        "stationId": station_id,
        "stationName": name,
        "rackTotCnt": rack,
        "parkingBikeTotCnt": "2",
        "shared": shared,
        "stationLatitude": lat,
        "stationLongitude": lon,
    }


def _body(rows):
    """`bikeList`의 실제 응답 모양.

    ⚠️ `list_total_count`는 **전체 대여소 수가 아니라 그 페이지의 행 수**다(실측:
    1/1000 -> 1000, 1001/2000 -> 1000, 2001/3000 -> 739). 같은 열린데이터광장이라도
    `bikeStationMaster`는 전체(3428)를 주므로 여기에 맞춰 페이지를 끝내면 안 된다.
    """
    return json.dumps({
        "rentBikeStatus": {
            "list_total_count": len(rows),
            "RESULT": {"CODE": "INFO-000", "MESSAGE": "ok"},
            "row": rows,
        }
    }).encode()


def _client(pages):
    """페이지 번호(1부터) 순서대로 행 묶음을 돌려주는 stub 클라이언트."""

    def handler(request):
        start = int(str(request.url).rstrip("/").rsplit("/", 2)[-2])
        index = (start - 1) // 1000
        rows = pages[index] if index < len(pages) else []
        return httpx.Response(200, content=_body(rows))

    return httpx.Client(transport=httpx.MockTransport(handler))


def _write_history(tmp_path, body, name="서울특별시 공공자전거 대여이력 정보_2501.csv"):
    (tmp_path / name).write_text(HISTORY_HEADER + body, encoding="cp949")
    return tmp_path


@pytest.fixture(autouse=True)
def _api_key(monkeypatch):
    monkeypatch.setenv("SEOUL_OPENAPI_KEY", "secret-key-123")


class TestApiLookup:
    def test_matches_by_exact_station_name(self, tmp_path):
        client = _client([[_station("ST-4", "102. 망원역 1번출구 앞")]])

        table = build(None, client=client)

        assert table.lookup("00102", "102. 망원역 1번출구 앞").station_id == "ST-4"

    def test_fills_every_column_from_api(self, tmp_path):
        client = _client([[_station("ST-4", "102. 망원역 1번출구 앞")]])

        info = build(None, client=client).lookup("00102", "102. 망원역 1번출구 앞")

        assert (info.rack_tot_cnt, info.shared) == ("15", "13")
        assert (info.latitude, info.longitude) == ("37.55", "126.91")

    def test_matches_by_name_prefix_when_name_changed(self):
        """대여소명이 미세하게 바뀌어도 접두 번호로 잡는다(실측 8개소)."""
        client = _client([[_station("ST-4", "102. 망원역 1번 출구 앞")]])

        info = build(None, client=client).lookup("00102", "102. 망원역 1번출구 앞")

        assert info.station_id == "ST-4"

    def test_pads_prefix_number_to_five_digits(self):
        """CSV의 `대여소번호`는 5자리 zero-padded인데 이름 접두는 `102`처럼 온다."""
        client = _client([[_station("ST-9", "1026. 대명초교 입구 교차로")]])

        info = build(None, client=client).lookup("01026", "다른 이름")

        assert info.station_id == "ST-9"

    def test_follows_pagination(self):
        client = _client([
            [_station(f"ST-{i}", f"{i}. 대여소") for i in range(1, 1001)],
            [_station("ST-2000", "2000. 마지막 대여소")],
        ])

        table = build(None, client=client)

        assert table.lookup("02000", "2000. 마지막 대여소").station_id == "ST-2000"


    def test_keeps_paging_while_pages_are_full(self):
        """`list_total_count`가 페이지 건수라 그것으로 끝을 판단하면 1,000개에서 멈춘다."""
        client = _client([
            [_station(f"ST-{i}", f"{i}. 대여소") for i in range(1, 1001)],
            [_station(f"ST-{i}", f"{i}. 대여소") for i in range(1001, 2001)],
            [_station(f"ST-{i}", f"{i}. 대여소") for i in range(2001, 2738)],
        ])

        table = build(None, client=client)

        assert table.stats["api_stations"] == 2737

class TestHistoryFallback:
    def test_fills_station_id_from_rental_history(self, tmp_path):
        """폐쇄된 대여소는 bikeList에 없다 — 대여이력 CSV로 stationId만 살린다."""
        d = _write_history(tmp_path,
            "SPB-1,2025-01-01 00:00:00,00211,211. 여의도역,02191,대학동,ST-99,ST-2375\n")
        client = _client([[_station("ST-4", "102. 망원역 1번출구 앞")]])

        info = build(d, client=client).lookup("00211", "211. 여의도역 4번출구 옆")

        assert info.station_id == "ST-99"

    def test_leaves_other_columns_empty_for_history_only_stations(self, tmp_path):
        """대여이력에는 거치대 수·거치율·좌표가 없다. 지어내지 않는다."""
        d = _write_history(tmp_path,
            "SPB-1,2025-01-01 00:00:00,00211,211. 여의도역,02191,대학동,ST-99,ST-2375\n")
        client = _client([[]])

        info = build(d, client=client).lookup("00211", "211. 여의도역")

        assert (info.rack_tot_cnt, info.shared, info.latitude, info.longitude) == ("", "", "", "")

    def test_reads_return_station_columns_too(self, tmp_path):
        d = _write_history(tmp_path,
            "SPB-1,2025-01-01 00:00:00,00211,211. 여의도역,02191,대학동,ST-99,ST-2375\n")
        client = _client([[]])

        info = build(d, client=client).lookup("02191", "대학동주민센터")

        assert info.station_id == "ST-2375"

    def test_api_wins_over_history(self, tmp_path):
        """API는 컬럼 5개를 모두 채우므로 stationId만 주는 이력보다 낫다."""
        d = _write_history(tmp_path,
            "SPB-1,2025-01-01 00:00:00,00102,102. 망원역,02191,대학동,ST-WRONG,ST-2375\n")
        client = _client([[_station("ST-4", "102. 망원역 1번출구 앞")]])

        info = build(d, client=client).lookup("00102", "102. 망원역 1번출구 앞")

        assert info.station_id == "ST-4"

    def test_skips_history_when_no_csv_dir_given(self):
        client = _client([[_station("ST-4", "102. 망원역 1번출구 앞")]])

        table = build(None, client=client)

        assert table.lookup("00211", "211. 여의도역") is None

    def test_ignores_csv_files_of_other_sources(self, tmp_path):
        """같은 디렉터리에 재고 CSV·기상 CSV가 섞여 있어도 대여이력만 읽는다."""
        (tmp_path / "weather_realtime_2025.csv").write_text("일시,기온\n2025-01-01 00:00,3\n", encoding="cp949")
        client = _client([[]])

        table = build(tmp_path, client=client)

        assert table.lookup("00211", "211. 여의도역") is None


class TestHistoryIdConflict:
    """대여소번호는 폐기 후 재사용될 수 있어 station_id와 1:1이 아니다."""

    def _two_periods(self, tmp_path):
        """같은 `00102`가 2412에는 ST-4, 2501에는 ST-999로 나오는 두 파일."""
        _write_history(tmp_path,
            "SPB-1,2024-12-01 00:00:00,00102,102. 망원역,02191,대학동,ST-4,ST-2375\n",
            name="서울특별시 공공자전거 대여이력 정보_2412.csv")
        _write_history(tmp_path,
            "SPB-2,2025-01-01 00:00:00,00102,102. 망원역,02191,대학동,ST-999,ST-2375\n",
            name="서울특별시 공공자전거 대여이력 정보_2501.csv")
        return tmp_path

    def test_records_conflict_with_period_trail(self, tmp_path):
        """조용히 덮어쓰지 않고 어느 기간에서 어느 기간으로 바뀌었는지 남긴다."""
        d = self._two_periods(tmp_path)
        client = _client([[]])

        table = build(d, client=client)

        assert table.stats["history_id_conflicts"] == 1
        assert table.stats["history_id_conflict_sample"] == ["00102: ST-4(2412) -> ST-999(2501)"]

    def test_latest_period_wins(self, tmp_path):
        """최신 id가 지금 살아있는 대여소를 가리킬 가능성이 높다 — 동작은 유지한다."""
        d = self._two_periods(tmp_path)
        client = _client([[]])

        info = build(d, client=client).lookup("00102", "102. 망원역")

        assert info.station_id == "ST-999"

    def test_warns_when_conflict_found(self, tmp_path, caplog):
        d = self._two_periods(tmp_path)
        client = _client([[]])

        with caplog.at_level(logging.WARNING, logger="bootstrap.station_join"):
            build(d, client=client)

        assert "00102: ST-4(2412) -> ST-999(2501)" in caplog.text

    def test_reports_no_conflict_when_ids_are_stable(self, tmp_path):
        """같은 번호가 같은 id로만 나오면 충돌이 아니다 — 정상 로그를 오염시키지 않는다."""
        _write_history(tmp_path,
            "SPB-1,2024-12-01 00:00:00,00102,102. 망원역,02191,대학동,ST-4,ST-2375\n",
            name="서울특별시 공공자전거 대여이력 정보_2412.csv")
        _write_history(tmp_path,
            "SPB-2,2025-01-01 00:00:00,00102,102. 망원역,02191,대학동,ST-4,ST-2375\n",
            name="서울특별시 공공자전거 대여이력 정보_2501.csv")
        client = _client([[]])

        table = build(tmp_path, client=client)

        assert table.stats["history_id_conflicts"] == 0
        assert table.stats["history_id_conflict_sample"] == []

    def test_reports_no_conflict_when_history_is_skipped(self):
        client = _client([[_station("ST-4", "102. 망원역 1번출구 앞")]])

        table = build(None, client=client)

        assert table.stats["history_id_conflicts"] == 0


class TestUnknownStation:
    def test_returns_none_when_nothing_matches(self):
        client = _client([[_station("ST-4", "102. 망원역 1번출구 앞")]])

        table = build(None, client=client)

        assert table.lookup("09999", "없는 대여소") is None


class TestStats:
    def test_counts_sources_and_match_kinds(self, tmp_path):
        d = _write_history(tmp_path,
            "SPB-1,2025-01-01 00:00:00,00211,211. 여의도역,02191,대학동,ST-99,ST-2375\n")
        client = _client([[
            _station("ST-4", "102. 망원역 1번출구 앞"),
            _station("ST-9", "1026. 대명초교 입구 교차로"),
        ]])

        table = build(d, client=client)

        assert table.stats["api_stations"] == 2
        assert table.stats["history_stations"] == 2

    def test_records_built_at(self, tmp_path):
        client = _client([[_station("ST-4", "102. 망원역 1번출구 앞")]])

        table = build(None, client=client)

        assert table.stats["built_at"].endswith("+09:00")
