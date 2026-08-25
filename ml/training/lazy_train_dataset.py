"""날짜 파티션 단위로 S3를 지연 조회하는 LightGBM 학습 데이터 계층.

**왜 필요한가**: multi-horizon feature 테이블은 20분 base/anchor
grid·full horizon·1년 실측만으로도 약 8억 행(13개 feature 컬럼)이었다.
5분 base 또는 anchor를 선택하면 조합에 따라 이보다 더 조밀해진다. 하나의
pandas DataFrame으로 통째로 읽으면(예전 `train_common.load_training_table()`이
하던 방식) 원본만 수십GB라 로컬(RAM 18GB)에서 반복적으로 OOM이 났다.
기본 g20/r20/a20이나 선택적 g5/r5/a20·g5/r5/a5 어느 경우에도 전체 날짜와
horizon 12개를 가능한 보존하려면, 근본 해결책은 그 DataFrame을 애초에 통째로
만들지 않는 것이다(`training/config.py` 주석 참고).

**설계 — LightGBM `lgb.Sequence` 두 단계 접근 패턴을 그대로 이용**(직접 재현해
검증, 코드 작성 전 확인):
`Dataset.construct()`는 각 Sequence를 (1) 샘플링 단계 — `bin_construct_sample_cnt`
표본을 Sequence별로 순차 방문하며 개별 정수 인덱스로, (2) 적재 단계 — 다시 순서대로
방문하며 `batch_size` 단위 연속 슬라이스로 접근한다. 두 단계 모두 "청크0 전부 →
청크1 전부 → …" 순서로 진행되고 섞이지 않는다. 그래서 청크(날짜) 하나의 원본을
`ChunkCache`(LRU, 기본 최대 2개)에 캐싱해두면, 캐시가 비워진 뒤 같은 청크가 다시
필요해질 때만(날짜당 최대 2회) 재조회하고, 항상 최대 1~2개 날짜분만 메모리에 남는다
— 메모리 대신 네트워크 I/O를 쓰는 의도된 트레이드오프.

`lgb.Sequence`는 원본 배열이 **float64여야만** 동작한다(직접 검증, float32는
`ValueError`). 이 변환은 청크(한 날짜, 최대 수백만 행) 단위로만 일어나므로 S3에
저장된 스키마(`ml_core.model_contract.NATIVE_COLUMN_DTYPES`의 int8/int16/float32
다운캐스트)는 그대로 두고, 학습 직전 그 청크에서만 잠깐 float64로 승격한다.

train/valid 둘 다 `build_lazy_dataset()`으로 Sequence 기반 `lgb.Dataset`을 만든다
(train Dataset과 valid Dataset을 둘 다 Sequence로 백엔드해도 조기 종료(early
stopping)까지 포함해 eager 버전과 예측값이 byte-identical함을 직접 검증). test는
`lgb.Dataset`으로 쓰이지 않고(학습 없이 `predict()`/지표 계산에만 쓰임) `Dataset`이
아니라 `predict_over_dates()`로 처리한다 — 날짜별로 그 청크만 읽어 즉시 predict한
뒤, 큰 feature 행렬은 버리고 작은(1D) 예측값/라벨 배열만 이어붙인다. valid도 학습
후 conformal correction 계산에 같은 함수를 다시 쓴다(학습용 Sequence 적재와는 별개
시점이라 청크를 다시 읽음 — 같은 트레이드오프).
"""

import os
import tempfile
import zlib
from collections import OrderedDict
from collections.abc import Callable, Iterable
from datetime import date

import lightgbm as lgb
import numpy as np
import pandas as pd
from core import s3 as s3_io
from ml_core.day_index import day_index
from ml_core.holidays_kr import korean_holidays
from ml_core.profile_contract import (
    DEFAULT_HOLIDAY_PEAK_HOURS,
    DEFAULT_MODEL_GRID_TICK_MINUTES,
    DEFAULT_WEEKDAY_PEAK_HOURS,
)

from . import config


def _is_in_peak_hours(minutes: np.ndarray, peak_hours: tuple[tuple[int, int], ...]) -> np.ndarray:
    """주어진 경과분(0~1439) 배열이 피크 시간대 구간 중 하나에 속하는지 판별한다."""
    if not peak_hours:
        return np.zeros_like(minutes, dtype=bool)
    mask = np.zeros_like(minutes, dtype=bool)
    for start_h, end_h in peak_hours:
        mask |= (minutes >= start_h * 60) & (minutes < end_h * 60)
    return mask


def _is_holiday_date(dt: date) -> bool:
    """주어진 날짜가 주말(토/일)이거나 대한민국 공휴일인지 판별한다."""
    if dt.weekday() >= 5:
        return True
    try:
        return dt.isoformat() in korean_holidays(dt.year)
    except Exception:
        return False


def _adaptive_anchor_mask(
    minute_series: pd.Series,
    is_night_day: bool,
    is_holiday: bool = False,
    weekday_peak_hours: tuple[tuple[int, int], ...] = DEFAULT_WEEKDAY_PEAK_HOURS,
    holiday_peak_hours: tuple[tuple[int, int], ...] = DEFAULT_HOLIDAY_PEAK_HOURS,
    peak_tick_minutes: int = DEFAULT_MODEL_GRID_TICK_MINUTES,
    regular_tick_minutes: int = 60,
    night_tick_minutes: int = 60,
) -> np.ndarray:
    """시간대별 가변 앵커링 불리언 마스크를 계산한다.

    - 피크 시간대(평일: weekday_peak_hours, 휴일: holiday_peak_hours): peak_tick_minutes 단위 전체 앵커 (minute % peak_tick_minutes == 0)
    - 평시 시간대: regular_tick_minutes 단위 정시 앵커 (minute % regular_tick_minutes == 0)
    - 심야 시간대(00~06시, minute < 360): 3일에 1번(is_night_day=True)만 night_tick_minutes 단위 정시 앵커 (minute % night_tick_minutes == 0)

    args:
        minute_series: 자정 기준 경과분(0~1439) Series
        is_night_day: 심야 앵커를 포함할 날짜인지 여부(3일에 1회)
        is_holiday: 주말 또는 공휴일 여부
        weekday_peak_hours: 평일 피크 시간대 구간 목록
        holiday_peak_hours: 휴일 피크 시간대 구간 목록
        peak_tick_minutes: 피크 시간대 샘플링 간격(분, 기본값 TRAIN_ANCHOR_TICK_MINUTES/20분)
        regular_tick_minutes: 평시 주간 샘플링 간격(분, 기본값 60분)
        night_tick_minutes: 심야 샘플링 간격(분, 기본값 60분)
    returns:
        np.ndarray: 유효 앵커 여부 불리언 마스크 배열
    """
    minutes = minute_series.to_numpy()
    peak_hours = holiday_peak_hours if is_holiday else weekday_peak_hours
    peak_mask = _is_in_peak_hours(minutes, peak_hours)

    night_mask = minutes < 360
    regular_mask = ~peak_mask & ~night_mask

    valid_peak = peak_mask & (minutes % peak_tick_minutes == 0)
    valid_regular = regular_mask & (minutes % regular_tick_minutes == 0)
    valid_night = (night_mask & (minutes % night_tick_minutes == 0)) if is_night_day else np.zeros_like(night_mask, dtype=bool)

    return valid_peak | valid_regular | valid_night


def _apply_adaptive_anchor_filter(
    df: pd.DataFrame,
    date_str: str,
    peak_tick_minutes: int | None = None,
) -> pd.DataFrame:
    """시간대별 가변 앵커링 필터를 적용해 유효한 앵커 행만 남긴다.

    args:
        df: 날짜 파티션 DataFrame
        date_str: 파티션 날짜 문자열("YYYY-MM-DD")
        peak_tick_minutes: 피크 시간대 샘플링 간격(None이면 config 설정 사용)
    returns:
        pd.DataFrame: 가변 앵커 필터가 적용된 DataFrame
    """
    if "minute" not in df.columns:
        return df
    try:
        dt = date.fromisoformat(date_str)
        is_night_day = day_index(dt) % 3 == 0
        is_holiday = _is_holiday_date(dt)
    except Exception:
        is_night_day = True
        is_holiday = False

    resolved_peak_tick = (
        peak_tick_minutes
        if peak_tick_minutes is not None
        else getattr(config, "PEAK_ANCHOR_TICK_MINUTES", getattr(config, "TRAIN_ANCHOR_TICK_MINUTES", DEFAULT_MODEL_GRID_TICK_MINUTES))
    )

    mask = _adaptive_anchor_mask(
        df["minute"],
        is_night_day=is_night_day,
        is_holiday=is_holiday,
        weekday_peak_hours=config.WEEKDAY_PEAK_HOURS,
        holiday_peak_hours=config.HOLIDAY_PEAK_HOURS,
        peak_tick_minutes=resolved_peak_tick,
    )
    return df[mask]


class ChunkCache:
    """날짜별 원본 numpy 배열을 최대 `max_size`개까지만 들고 있는 LRU.

    모듈 docstring의 "왜 max_size=2인가" 참고 — 1개로도 정확성은 유지되지만, 인접한
    두 청크가 잠깐 겹쳐 필요한(예: 적재 단계 슬라이스가 청크 경계를 걸치는) 극단적인
    경우를 대비해 2로 둔다. 크게 늘리면 그만큼 여러 날짜분이 동시에 상주해 원래
    문제가 다시 재발하므로 임의로 키우지 않는다.
    """

    def __init__(self, max_size: int = 2):
        self._max_size = max_size
        self._data: OrderedDict[str, np.ndarray] = OrderedDict()

    def get_or_fetch(self, key: str, loader: Callable[[], np.ndarray]) -> np.ndarray:
        """캐시에 있으면 재사용하고, 없으면 로드해 LRU 크기를 유지한다."""
        if key in self._data:
            self._data.move_to_end(key)
            return self._data[key]
        value = loader()
        self._data[key] = value
        if len(self._data) > self._max_size:
            self._data.popitem(last=False)
        return value

    def clear(self) -> None:
        """상주 중인 날짜 feature 배열을 모두 해제한다."""
        self._data.clear()


def _list_date_part_keys(table_path: str, date_str: str) -> list[str]:
    """`core.s3._read_parquet_by_dates()`와 정확히 같은 방식으로 날짜 하나의 part 파일 키를 나열한다.

    같은 정렬 기준(`sorted()`)을 써야 한다 — 파케이 파일 자체는 불변이므로 나열
    순서만 같으면 이 함수를 몇 번을 다시 불러도(캐시 miss로 인한 재조회 포함) 항상
    같은 행 순서를 재현할 수 있다. 순서가 어긋나면 나중에 합치는 라벨과 feature가
    서로 다른 행끼리 짝지어지는 조용한 오류가 난다.
    """
    prefix = table_path if table_path.endswith("/") else f"{table_path}/"
    return sorted(k for k in s3_io.list_keys(f"{prefix}date={date_str}/") if k.endswith(".parquet"))


def _feature_frame_to_float64(
    df: pd.DataFrame, feature_columns: list[str], station_dtype: pd.CategoricalDtype
) -> np.ndarray:
    """feature_columns 순서 그대로, station_no만 카테고리 코드로 바꿔 float64 2D 배열을 만든다.

    `lgb.Sequence`/일반 numpy 배열 경로 둘 다 float64만 받으므로(모듈 docstring
    참고) 여기서 한 번에 변환한다. station_dtype은 train/valid/test 전체에서 미리
    고정해 넘겨줘야 한다 — split마다 따로 카테고리를 매기면 코드가 어긋난다(기존
    `train_common._prepare_xy()`와 같은 이유).
    """
    arr = np.empty((len(df), len(feature_columns)), dtype=np.float64)
    for i, col in enumerate(feature_columns):
        if col == "station_no":
            arr[:, i] = df[col].astype(station_dtype).cat.codes.to_numpy().astype(np.float64)
        else:
            arr[:, i] = df[col].to_numpy(dtype=np.float64)
    return arr


def _read_date_chunk(
    table_path: str,
    date_str: str,
    columns: list[str],
    filters: list[tuple] | None,
    adaptive_anchors: bool | None = None,
    station_shard: frozenset[int] | None = None,
) -> pd.DataFrame:
    """날짜 하나(`date=YYYY-MM-DD/` 파티션)만 읽어 pandas DataFrame으로 반환한다.

    `core.s3.list_keys()`/`read_parquet_many()`를 그대로 재사용한다(새 S3 접근 계층을
    만들지 않음) — `core.s3._read_parquet_by_dates()`가 여러 날짜에 걸쳐 하는 일을
    날짜 하나로 좁힌 버전이다.

    `adaptive_anchors`가 True(기본값 config.ADAPTIVE_TRAIN_ANCHORS)이면 피크(20분)/
    평시(60분)/심야(3일 1회 60분) 가변 앵커링 필터를 적용한다.

    `station_shard`가 주어지면(분산 학습, `LGB_NUM_MACHINES>1`) 이 머신이 담당하는
    station_no만 남긴다(`_shard_for_this_machine()` 참고) — 라벨 prepass
    (`_stream_prepass_arrays()`)와 feature 청크(`_DatePartitionSequence`)가 이
    함수를 통해 **같은 필터를 같은 행 순서로** 적용받으므로, 서로 다른 시점에
    독립적으로 다시 읽어도 행이 어긋나지 않는다.
    """
    part_keys = _list_date_part_keys(table_path, date_str)
    if not part_keys:
        raise FileNotFoundError(f"S3에 이 날짜 파티션이 없음: {table_path}/date={date_str}/")

    use_adaptive = config.ADAPTIVE_TRAIN_ANCHORS if adaptive_anchors is None else adaptive_anchors
    read_columns = list(columns)
    temp_minute_added = False
    temp_station_no_added = False
    if use_adaptive and "minute" not in read_columns:
        first_bytes = s3_io.get_object_bytes(part_keys[0])
        if first_bytes is not None:
            import io
            import pyarrow.parquet as pq

            try:
                schema_names = pq.read_schema(io.BytesIO(first_bytes)).names
                if "minute" in schema_names:
                    read_columns.append("minute")
                    temp_minute_added = True
            except Exception:
                pass
    if station_shard is not None and "station_no" not in read_columns:
        read_columns.append("station_no")
        temp_station_no_added = True

    tables = [
        t for t in s3_io.read_parquet_many(part_keys, columns=read_columns, as_pandas=False, filters=filters) if t is not None
    ]
    if not tables:
        raise FileNotFoundError(f"S3에 이 날짜 파티션에 데이터가 없음(filters 이후 0행): {table_path}/date={date_str}/")
    combined = s3_io.concat_compatible_tables(tables, required_columns=read_columns)
    del tables
    df = combined.to_pandas(self_destruct=True, split_blocks=True)
    del combined

    if use_adaptive and "minute" in df.columns:
        df = _apply_adaptive_anchor_filter(df, date_str)
        if temp_minute_added:
            df = df[[c for c in columns if c in df.columns] + (["station_no"] if temp_station_no_added else [])]

    if station_shard is not None and "station_no" in df.columns:
        df = df[df["station_no"].isin(station_shard)]
        if temp_station_no_added:
            df = df[[c for c in columns if c in df.columns]]

    if df.empty:
        raise FileNotFoundError(
            f"S3에 이 날짜 파티션에 데이터가 없음(filters/adaptive anchors/station shard 이후 0행): "
            f"{table_path}/date={date_str}/"
        )
    return df


def _warn_missing_date_chunk(
    table_path: str,
    date_str: str,
    exc: FileNotFoundError,
    on_missing_date: Callable[[str], None] | None = None,
) -> None:
    """날짜 파티션 하나가 없거나(전날 feature mart 생성 실패 등) 필터 이후 0행이면
    호출부는 그 날짜를 0행으로 취급하고 건너뛴다 — 한 달 학습이 하루치 결측 때문에
    통째로 실패하면 안 되기 때문이다. 다만 조용히 넘기면 결측을 아무도 모르게 되므로
    표준출력에 경고를 남긴다(이 파일/`train_common.py`가 공통으로 쓰는 print 기반 로그
    관례). 전체 날짜가 다 비었을 때만(호출부의 총 행 수 0 체크) 실제로 실패한다.

    `on_missing_date`가 주어지면 이 날짜 문자열로 한 번 더 호출한다 — 이 파일은
    mlflow를 모르므로(관심사 분리), `train_common.py`가 이 콜백으로 결측 날짜를
    모아서 MLflow run에 기록한다(그래야 "일부 날짜만으로 학습했다"는 사실이 학습
    로그를 뒤지지 않아도 MLflow에서 바로 보인다).
    """
    print(f"[lazy_train_dataset] 경고: {table_path} date={date_str} 건너뜀 — {exc}", flush=True)
    if on_missing_date is not None:
        on_missing_date(date_str)


class _DatePartitionSequence(lgb.Sequence):
    """multi-horizon 테이블의 날짜 파티션 하나를 표현하는 `lgb.Sequence`.

    `__getitem__`이 호출될 때만(그것도 공유 `cache`를 거쳐 최초 1회만) 실제로 S3를
    읽는다 — `lgb.Dataset(data=[seq0, seq1, ...])`에 이 객체들을 리스트로 넘기면
    LightGBM이 필요한 시점에만 순서대로 접근한다(모듈 docstring의 두 단계 접근 패턴).
    """

    def __init__(
        self,
        table_path: str,
        date_str: str,
        feature_columns: list[str],
        station_dtype: pd.CategoricalDtype,
        filters: list[tuple] | None,
        row_count: int,
        cache: ChunkCache,
        batch_size: int = 200_000,
        on_chunk_loaded: Callable[[str, int], None] | None = None,
        station_shard: frozenset[int] | None = None,
    ):
        self._table_path = table_path
        self._date_str = date_str
        self._feature_columns = feature_columns
        self._station_dtype = station_dtype
        self._filters = filters
        self._row_count = row_count
        self._cache = cache
        self.batch_size = batch_size  # lgb.Sequence가 적재 단계에서 읽는 슬라이스 크기
        self._on_chunk_loaded = on_chunk_loaded
        self._station_shard = station_shard

    def __len__(self) -> int:
        return self._row_count

    def __getitem__(self, idx):
        return self._cache.get_or_fetch(self._date_str, self._load)[idx]

    def _load(self) -> np.ndarray:
        df = _read_date_chunk(
            self._table_path, self._date_str, self._feature_columns, self._filters,
            station_shard=self._station_shard,
        )
        arr = _feature_frame_to_float64(df, self._feature_columns, self._station_dtype)
        del df
        if self._on_chunk_loaded is not None:
            self._on_chunk_loaded(self._date_str, len(arr))
        return arr


def station_categories_for_dates(
    table_path: str,
    dates: list[str],
    filters: list[tuple] | None,
    on_complete: Callable[[int, int], None] | None = None,
    on_missing_date: Callable[[str], None] | None = None,
) -> list[int]:
    """날짜별로 station_no를 읽고 즉시 set에 합쳐 전역 카테고리를 반환한다.

    train_target()이 학습을 시작하기 전에 train+valid+test 날짜를 합쳐 한 번만
    호출해서 전역 station_dtype을 고정한다 — station_no 카테고리 코드는 세 split
    전체에서 같은 매핑을 써야 LightGBM이 같은 station을 같은 코드로 본다(기존
    `train_common._prepare_xy()`와 같은 이유). 전체 기간의 station_no 열을 하나의
    pandas DataFrame으로 합치지 않아 행 수가 늘어도 peak memory는 하루치로 제한된다.

    args:
        on_missing_date: `_warn_missing_date_chunk()` 참고 — 결측 날짜를 MLflow에
            남기고 싶은 호출부(`train_common.py`)가 넘긴다.
    """
    categories: set[int] = set()
    for done, date_str in enumerate(dates, start=1):
        try:
            df = _read_date_chunk(
                table_path,
                date_str,
                ["station_no"],
                filters,
            )
        except FileNotFoundError as exc:
            _warn_missing_date_chunk(table_path, date_str, exc, on_missing_date)
            df = None
        if df is not None:
            categories.update(int(value) for value in df["station_no"].unique())
            del df
        if on_complete is not None:
            on_complete(done, len(dates))
    if not categories:
        raise FileNotFoundError(f"S3에 없음: {table_path} (dates 예: {dates[:3]})")
    return sorted(categories)


def _shard_for_this_machine(
    station_categories: Iterable[int], num_machines: int, machine_rank: int
) -> frozenset[int]:
    """전체 station_no 목록을 `num_machines`개 머신에 나눠 이 머신(`machine_rank`) 몫만 반환한다.

    LightGBM 소켓 분산(`tree_learner="data"`)은 전체 데이터를 자동으로 나눠주지
    않는다 — 각 머신이 미리 자기 몫만 들고 `lgb.train()`을 호출해야 한다
    (ADR-0005/0007 참고). `hash()` 내장 함수 대신 `zlib.crc32`를 쓰는 이유는
    `PYTHONHASHSEED`가 프로세스마다 랜덤이라 `hash()`를 쓰면 같은 station_no가
    머신마다 다른 값으로 해시돼 배정이 어긋날 수 있기 때문이다 — `zlib.crc32`는
    입력 바이트에 대해 항상 결정적이다.

    **호출 시점 주의**: 이 함수는 실제 학습 행을 읽을 때만 써야 한다 —
    `station_categories_for_dates()`(station_no `CategoricalDtype`을 고정하는
    전역 스캔)에는 절대 적용하면 안 된다. 머신마다 다른 station 부분집합만 보고
    카테고리를 고정하면 머신 간 카테고리 코드가 어긋나서, 승격된 모델을 읽는
    inference가 station_no를 조용히 잘못 해석하게 된다.

    args:
        station_categories: 전체 station_no 목록(보통 `station_categories_for_dates()`의 반환값)
        num_machines: 전체 분산 학습 머신 수(`LGB_NUM_MACHINES`)
        machine_rank: 이 프로세스의 0-based 순번(`LGB_MACHINE_RANK`)
    returns:
        frozenset[int]: 이 머신이 담당할 station_no 집합
    """
    return frozenset(
        station_no
        for station_no in station_categories
        if zlib.crc32(str(int(station_no)).encode()) % num_machines == machine_rank
    )


def _open_unlinked_memmap(file_obj, row_count: int) -> np.memmap:
    """작성 완료한 임시 파일을 float64 memmap으로 열고 디렉터리 항목은 제거한다.

    Linux에서 열린 mmap은 파일을 unlink한 뒤에도 마지막 참조가 사라질 때까지
    유효하다. 정상·예외 종료 모두 거대한 prepass 파일이 남지 않게 하면서 label과
    exposure를 RAM 대신 로컬 scratch에 둔다.

    args:
        file_obj: float64 raw bytes를 쓴 열린 임시 파일.
        row_count: 배열 원소 수.
    returns:
        삭제 예약된 임시 파일을 backing store로 쓰는 1차원 memmap.
    """
    path = file_obj.name
    file_obj.flush()
    os.fsync(file_obj.fileno())
    file_obj.close()
    mapped = np.memmap(path, dtype=np.float64, mode="r+", shape=(row_count,))
    os.unlink(path)
    return mapped


def _empty_unlinked_memmap(prefix: str, row_count: int) -> np.memmap:
    """지정한 길이의 삭제 예약된 float64 scratch 배열을 만든다."""
    file_obj = tempfile.NamedTemporaryFile(prefix=prefix, suffix=".bin", delete=False)
    path = file_obj.name
    try:
        file_obj.truncate(row_count * np.dtype(np.float64).itemsize)
        return _open_unlinked_memmap(file_obj, row_count)
    except Exception:
        if not file_obj.closed:
            file_obj.close()
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        raise


class LazySequenceDataset(lgb.Dataset):
    """날짜 Sequence를 chunk 단위 init score로 이어 학습하는 Dataset이다.

    LightGBM의 기본 continuation 경로는 ``data=[Sequence, ...]``를 하나의 NumPy
    배열로 바꾸려 한다. 날짜마다 행 수가 다른 운영 데이터에서는 실패하고, 억지로
    합치면 전체 feature 원본이 동시에 상주한다. 이 클래스는 이전 Booster의 raw
    score를 날짜별로 계산해 disk-backed 1차원 배열에 이어 쓰고 native Dataset에
    복사한다. 원래 init score(exposure offset)가 있으면 이전 tree raw score에 더해
    중단 전 학습 의미를 그대로 복원한다.
    """

    _resume_base_init_score: np.ndarray | None = None
    _resume_init_score_attached: bool = False

    def _set_init_score_by_predictor(self, predictor, data, used_indices):
        """lazy Sequence 목록은 전체 결합 없이 predictor init score를 설정한다."""
        if predictor is None and used_indices is None and self._resume_init_score_attached:
            return self._reset_resume_init_score()
        if predictor is None or used_indices is not None or not (
            isinstance(data, list)
            and data
            and all(isinstance(chunk, _DatePartitionSequence) for chunk in data)
        ):
            return super()._set_init_score_by_predictor(predictor, data, used_indices)
        if predictor.num_class != 1:
            raise NotImplementedError(
                "lazy checkpoint 재개는 현재 단일 출력 모델만 지원합니다: "
                f"num_class={predictor.num_class}"
            )

        num_data = self.num_data()
        init_score = _empty_unlinked_memmap("bike-resume-init-score-", num_data)
        base_init_score = self._resume_base_init_score
        if base_init_score is not None and len(base_init_score) != num_data:
            raise ValueError(
                "재개용 base init score 행 수가 Dataset과 다릅니다: "
                f"base={len(base_init_score):,}, dataset={num_data:,}"
            )

        offset = 0
        try:
            for chunk in data:
                feature_arr = chunk[:]
                chunk_size = len(feature_arr)
                stop = offset + chunk_size
                raw_score = np.asarray(
                    predictor.predict(feature_arr, raw_score=True),
                    dtype=np.float64,
                ).ravel()
                if len(raw_score) != chunk_size:
                    raise ValueError(
                        "재개 predictor 결과 행 수가 날짜 chunk와 다릅니다: "
                        f"predictions={len(raw_score):,}, chunk={chunk_size:,}"
                    )
                init_score[offset:stop] = raw_score
                if base_init_score is not None:
                    init_score[offset:stop] += base_init_score[offset:stop]
                offset = stop
                del feature_arr, raw_score
            if offset != num_data:
                raise ValueError(
                    "재개 init score 행 수가 Dataset과 다릅니다: "
                    f"predictions={offset:,}, dataset={num_data:,}"
                )
            # `set_init_score()`는 native field에 쓴 뒤 `self.init_score`에 전체 길이
            # float64 사본을 되읽는다(800M행이면 ~6.4GB). 여기는 lgb.train() 안이라
            # train/valid native storage가 이미 둘 다 상주한 시점이므로 그 사본이
            # 곧바로 peak를 밀어올린다. native field에만 직접 쓴다.
            self.set_field("init_score", init_score)
            self.init_score = None
            self._resume_init_score_attached = True
            return self
        finally:
            del init_score

    def _reset_resume_init_score(self):
        """phase가 바뀌어 predictor가 떨어질 때 재개용 init score를 걷어낸다.

        LightGBM 기본 구현은 ``self.init_score``가 살아 있을 때만 native field를
        0으로 덮는다(`Dataset._set_init_score_by_predictor`의 ``elif`` 분기). 위에서
        메모리 때문에 Python 사본을 비워두므로 그 경로가 그대로 통과해버리고,
        ``train_set``/``valid_set``을 phase 간 재사용하는 `train_common.train_target`
        에서는 앞 phase의 raw score가 다음 phase의 offset으로 남는다(예: q10을
        체크포인트에서 재개하면 q50/q90이 q10 예측 위에서 학습된다). 재사용 전에
        직접 원상복구한다 — 원래 offset(대여 poisson의 log(exposure))이 있으면 그
        값으로, 없으면 field 자체를 비운다.
        """
        self.set_field("init_score", self._resume_base_init_score)
        self.init_score = None
        self._resume_init_score_attached = False
        return self


def _stream_prepass_arrays(
    table_path: str,
    dates: list[str],
    filters: list[tuple] | None,
    label_col: str,
    exposure_col: str | None,
    on_complete: Callable[[int, int], None] | None,
    station_shard: frozenset[int] | None = None,
    on_missing_date: Callable[[str], None] | None = None,
) -> tuple[list[int], np.memmap, np.memmap | None]:
    """label/exposure를 날짜별로 읽어 삭제 예약된 disk-backed 배열에 이어 쓴다.

    전체 날짜를 Arrow/pandas로 합친 뒤 numpy로 다시 복사하던 경로는 2025년 대여
    train prepass 직후 process-tree RSS가 23.64GiB까지 치솟았다. 이 함수는 날짜
    하나를 변환한 즉시 raw float64 bytes로 scratch에 내리고 해제한다. 최종 배열은
    ``np.memmap``이라 LightGBM에 같은 1차원 numpy 계약을 제공하면서 peak RAM은
    하루치로 제한된다.

    args:
        on_missing_date: `_warn_missing_date_chunk()` 참고.
    returns:
        날짜별 행 수, label memmap, 선택적 exposure memmap.
    raises:
        ValueError: 선택한 날짜 전체가 비어 있을 때.
    """
    label_file = tempfile.NamedTemporaryFile(prefix="bike-label-", suffix=".bin", delete=False)
    exposure_file = (
        tempfile.NamedTemporaryFile(prefix="bike-exposure-", suffix=".bin", delete=False)
        if exposure_col
        else None
    )
    row_counts: list[int] = []
    total_rows = 0
    try:
        columns = [label_col] + ([exposure_col] if exposure_col else [])
        for done, date_str in enumerate(dates, start=1):
            try:
                df = _read_date_chunk(table_path, date_str, columns, filters, station_shard=station_shard)
            except FileNotFoundError as exc:
                _warn_missing_date_chunk(table_path, date_str, exc, on_missing_date)
                df = None
            row_count = 0 if df is None else len(df)
            row_counts.append(row_count)
            total_rows += row_count
            if df is not None:
                labels = df[label_col].to_numpy(dtype=np.float64)
                label_file.write(labels.tobytes(order="C"))
                if exposure_file is not None and exposure_col is not None:
                    exposures = df[exposure_col].to_numpy(dtype=np.float64)
                    exposure_file.write(exposures.tobytes(order="C"))
                    del exposures
                del labels, df
            if on_complete is not None:
                on_complete(done, len(dates))

        if total_rows == 0:
            raise ValueError(
                f"학습 구간에 데이터가 없음: {table_path} "
                f"dates={dates[:3]}...({len(dates)}개) — feature mart 확인 필요"
            )
        labels_mmap = _open_unlinked_memmap(label_file, total_rows)
        exposure_mmap = (
            _open_unlinked_memmap(exposure_file, total_rows)
            if exposure_file is not None
            else None
        )
        return row_counts, labels_mmap, exposure_mmap
    except Exception:
        for file_obj in (label_file, exposure_file):
            if file_obj is None:
                continue
            path = file_obj.name
            if not file_obj.closed:
                file_obj.close()
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
        raise


def _log_memmap(values: np.ndarray) -> np.memmap:
    """양수 exposure의 자연로그를 별도 삭제 예약된 disk-backed 배열로 만든다."""
    file_obj = tempfile.NamedTemporaryFile(prefix="bike-init-score-", suffix=".bin", delete=False)
    path = file_obj.name
    try:
        file_obj.truncate(values.size * np.dtype(np.float64).itemsize)
        mapped = _open_unlinked_memmap(file_obj, values.size)
        np.log(values, out=mapped)
        return mapped
    except Exception:
        if not file_obj.closed:
            file_obj.close()
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        raise


def build_lazy_dataset(
    table_path: str,
    dates: list[str],
    feature_columns: list[str],
    station_dtype: pd.CategoricalDtype,
    filters: list[tuple] | None,
    label_col: str,
    exposure_col: str | None,
    cache: ChunkCache,
    reference: lgb.Dataset | None = None,
    on_chunk_loaded: Callable[[str, int], None] | None = None,
    on_prepass_complete: Callable[[int, int], None] | None = None,
    dataset_params: dict | None = None,
    keep_raw_data: bool = False,
    station_shard: frozenset[int] | None = None,
    on_missing_date: Callable[[str], None] | None = None,
) -> tuple[lgb.Dataset, np.ndarray, np.ndarray | None]:
    """train 또는 valid 하나를 Sequence 기반 `lgb.Dataset`으로 만든다.

    1. 라벨(+exposure) 사전 스캔 — 날짜 하나씩 읽어 로컬 scratch의 삭제 예약된
       memmap에 이어 쓴다. `dates` 순서가 아래 Sequence 목록 순서와 반드시 같아야
       라벨이 어긋나지 않는다.
    2. 날짜별 행 수로 `_DatePartitionSequence` 리스트 생성(공유 `cache` 주입).
    3. `lgb.Dataset(data=sequences, ...)` 구성.

    args:
        dates: `_dates_for_split()`이 계산한 이 split의 날짜 목록(캘린더 연산만, 이미 확정됨)
        reference: valid Dataset을 만들 때 train Dataset을 넘긴다(빈 스토리지 재사용 —
            LightGBM이 같은 bin 경계를 쓰게 함). train 자신은 None.
        dataset_params: Dataset construct 시점에 고정돼야 하는 LightGBM 파라미터.
            `max_bin`/`min_data_in_leaf` 등이 이후 `lgb.train()`에서 뒤늦게 바뀌어
            거부되거나 다른 binning을 쓰지 않도록 학습 파라미터를 함께 넘긴다.
        keep_raw_data: checkpoint Booster를 `init_model`로 이어 학습할 때 LightGBM이
            원본 Sequence에 predictor를 연결할 수 있도록 보존할지 여부.
        station_shard: 분산 학습(`LGB_NUM_MACHINES>1`)에서 이 머신이 담당하는
            station_no 집합(`_shard_for_this_machine()`). None(기본값)이면 샤딩
            없이 전체를 읽는다(기존 단일 머신 동작과 동일).
        on_missing_date: `_warn_missing_date_chunk()` 참고 — 결측 날짜를 MLflow에
            남기고 싶은 호출부(`train_common.py`)가 넘긴다.
    returns:
        tuple[lgb.Dataset, np.ndarray, np.ndarray | None]: (construct()까지 끝난 Dataset, y, exposure)
    raises:
        ValueError: 이 날짜 구간에 실제 데이터가 하나도 없을 때(피처 마트가 아직
            안 쌓였을 수 있음) — `lgb.train()` 안에서 알아보기 힘든 에러로 죽기 전에
            여기서 먼저 걸러낸다.
    """
    row_counts, y, exposure = _stream_prepass_arrays(
        table_path,
        dates,
        filters,
        label_col,
        exposure_col,
        on_prepass_complete,
        station_shard=station_shard,
        on_missing_date=on_missing_date,
    )

    sequences = [
        _DatePartitionSequence(
            table_path, d, feature_columns, station_dtype, filters, rc, cache, on_chunk_loaded=on_chunk_loaded,
            station_shard=station_shard,
        )
        for d, rc in zip(dates, row_counts, strict=True)
        if rc > 0
    ]
    init_score = _log_memmap(exposure) if exposure is not None else None
    cat_idx = [feature_columns.index("station_no")]
    dataset = LazySequenceDataset(
        data=sequences,
        label=y,
        init_score=init_score,
        reference=reference,
        feature_name=feature_columns,
        categorical_feature=cat_idx,
        params=dataset_params,
        free_raw_data=not keep_raw_data,
    )
    dataset.construct()
    # construct()는 label/init_score를 native Dataset handle에 복사한 뒤 다시 numpy
    # field로 읽어 `dataset.label`/`dataset.init_score`에도 보관한다. 대규모 train은
    # 이 Python-side 복사본만 수 GB이고, 호출부가 보유한 memmap과 valid Dataset
    # 구성 시점에 겹치면 32GB 환경의 peak를 넘는다. 둘을 None으로 비워도
    # `get_label()`/`get_init_score()`가 필요할 때 native field에서 다시 읽을 수 있고,
    # `lgb.train()`은 이미 구성된 handle을 직접 사용한다.
    dataset.label = None
    dataset.init_score = None
    if keep_raw_data:
        dataset._resume_base_init_score = init_score
    else:
        del init_score
    return dataset, y, exposure


def predict_over_dates(
    table_path: str,
    dates: list[str],
    feature_columns: list[str],
    station_dtype: pd.CategoricalDtype,
    filters: list[tuple] | None,
    label_col: str,
    exposure_col: str | None,
    boosters: dict[str, lgb.Booster],
    on_chunk_loaded: Callable[[str, int], None] | None = None,
    station_shard: frozenset[int] | None = None,
    on_missing_date: Callable[[str], None] | None = None,
) -> dict:
    """test 전체, 또는 valid의 학습후 예측(conformal correction용)에 쓴다 — `lgb.Dataset`이 아니다.

    날짜별로 그 청크만 읽어 `boosters` 전부로 즉시 predict한 뒤, 큰 feature 행렬은
    버리고 작은(1D) 예측값/라벨/exposure 배열만 이어붙인다 — `X_test`/`X_valid`라는
    실체를 아예 만들지 않는다. 같은 날짜 구간을 학습 전(Sequence 적재)과 학습 후
    (이 함수)에 각각 다시 읽으므로 I/O는 늘지만(모듈 docstring의 트레이드오프),
    피크 메모리는 항상 청크 하나 크기로 유지된다.

    args:
        boosters: {이름: booster} — 이름은 반환 dict의 키로 그대로 쓰인다(예:
            {"poisson": booster} 또는 {"q10": ..., "q50": ..., "q90": ...})
        station_shard: 분산 학습에서 이 머신이 담당하는 station_no 집합. 주어지면
            이 머신 몫만 평가한다 — 분산 학습에서는 각 머신이 자기 shard로 학습한
            booster를 자기 shard의 valid/test로만 평가하는 것이 일관적이다(전체
            valid/test는 어느 한 머신에도 없음). rank 0의 결과만 저장되므로
            conformal correction/최종 metrics는 rank 0 shard 기준 근사치가
            된다(ADR-0005/0007에 문서화된 기존 한계).
        on_missing_date: `_warn_missing_date_chunk()` 참고 — 결측 날짜를 MLflow에
            남기고 싶은 호출부(`train_common.py`)가 넘긴다.
    returns:
        dict: {"y": ndarray, "exposure": ndarray | None, **{name: ndarray for name in boosters}}
    raises:
        ValueError: 이 날짜 구간에 실제 데이터가 하나도 없을 때
    """
    read_columns = [*feature_columns, label_col] + ([exposure_col] if exposure_col else [])
    y_parts: list[np.ndarray] = []
    exposure_parts: list[np.ndarray] = []
    pred_parts: dict[str, list[np.ndarray]] = {name: [] for name in boosters}

    for date_str in dates:
        try:
            chunk_df = _read_date_chunk(table_path, date_str, read_columns, filters, station_shard=station_shard)
        except FileNotFoundError as exc:
            _warn_missing_date_chunk(table_path, date_str, exc, on_missing_date)
            continue  # 이 날짜 파티션이 아예 없거나 filters/station shard 이후 0행 — 다음 날짜로

        y_parts.append(chunk_df[label_col].to_numpy(dtype=np.float64))
        if exposure_col:
            exposure_parts.append(chunk_df[exposure_col].to_numpy(dtype=np.float64))
        feature_arr = _feature_frame_to_float64(chunk_df, feature_columns, station_dtype)
        del chunk_df
        for name, booster in boosters.items():
            pred_parts[name].append(booster.predict(feature_arr, num_iteration=booster.best_iteration))
        chunk_len = len(feature_arr)
        del feature_arr
        if on_chunk_loaded is not None:
            on_chunk_loaded(date_str, chunk_len)

    if not y_parts:
        raise ValueError(f"학습 구간에 데이터가 없음: {table_path} dates={dates[:3]}...({len(dates)}개) — feature mart 확인 필요")

    result: dict = {"y": np.concatenate(y_parts)}
    result["exposure"] = np.concatenate(exposure_parts) if exposure_col else None
    for name in boosters:
        result[name] = np.concatenate(pred_parts[name])
    return result
