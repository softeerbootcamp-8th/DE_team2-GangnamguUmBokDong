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

from collections import OrderedDict
from collections.abc import Callable

import lightgbm as lgb
import numpy as np
import pandas as pd
from core import s3 as s3_io


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


def _read_date_chunk(table_path: str, date_str: str, columns: list[str], filters: list[tuple] | None) -> pd.DataFrame:
    """날짜 하나(`date=YYYY-MM-DD/` 파티션)만 읽어 pandas DataFrame으로 반환한다.

    `core.s3.list_keys()`/`read_parquet_many()`를 그대로 재사용한다(새 S3 접근 계층을
    만들지 않음) — `core.s3._read_parquet_by_dates()`가 여러 날짜에 걸쳐 하는 일을
    날짜 하나로 좁힌 버전이다.
    """
    part_keys = _list_date_part_keys(table_path, date_str)
    if not part_keys:
        raise FileNotFoundError(f"S3에 이 날짜 파티션이 없음: {table_path}/date={date_str}/")
    tables = [
        t for t in s3_io.read_parquet_many(part_keys, columns=columns, as_pandas=False, filters=filters) if t is not None
    ]
    if not tables:
        raise FileNotFoundError(f"S3에 이 날짜 파티션에 데이터가 없음(filters 이후 0행): {table_path}/date={date_str}/")
    combined = s3_io.concat_compatible_tables(tables, required_columns=columns)
    del tables
    df = combined.to_pandas(self_destruct=True, split_blocks=True)
    del combined
    return df


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

    def __len__(self) -> int:
        return self._row_count

    def __getitem__(self, idx):
        return self._cache.get_or_fetch(self._date_str, self._load)[idx]

    def _load(self) -> np.ndarray:
        df = _read_date_chunk(self._table_path, self._date_str, self._feature_columns, self._filters)
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
) -> list[int]:
    """주어진 날짜 전체에서 station_no 유니크 값만 골라 읽는다(컬럼 1개, 8억 행이어도 수백MB대).

    train_target()이 학습을 시작하기 전에 train+valid+test 날짜를 합쳐 한 번만
    호출해서 전역 station_dtype을 고정한다 — station_no 카테고리 코드는 세 split
    전체에서 같은 매핑을 써야 LightGBM이 같은 station을 같은 코드로 본다(기존
    `train_common._prepare_xy()`와 같은 이유).
    """
    df = s3_io.read_parquet(table_path, columns=["station_no"], dates=dates, filters=filters, on_complete=on_complete)
    if df is None:
        raise FileNotFoundError(f"S3에 없음: {table_path} (dates 예: {dates[:3]})")
    return sorted(int(s) for s in df["station_no"].unique())


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
) -> tuple[lgb.Dataset, np.ndarray, np.ndarray | None]:
    """train 또는 valid 하나를 Sequence 기반 `lgb.Dataset`으로 만든다.

    1. 라벨(+exposure) 사전 스캔 — 컬럼 1~2개뿐이라(+`date`, 날짜별로 다시 가르는 용도)
       8억 행이어도 수 GB대. `dates` 순서대로 이어붙여 전역 `y`/`exposure`를 만든다 —
       이 순서가 아래 Sequence 목록 순서와 반드시 일치해야 라벨이 안 어긋난다.
    2. 날짜별 행 수로 `_DatePartitionSequence` 리스트 생성(공유 `cache` 주입).
    3. `lgb.Dataset(data=sequences, ...)` 구성.

    args:
        dates: `_dates_for_split()`이 계산한 이 split의 날짜 목록(캘린더 연산만, 이미 확정됨)
        reference: valid Dataset을 만들 때 train Dataset을 넘긴다(빈 스토리지 재사용 —
            LightGBM이 같은 bin 경계를 쓰게 함). train 자신은 None.
        dataset_params: Dataset construct 시점에 고정돼야 하는 LightGBM 파라미터.
            `max_bin`/`min_data_in_leaf` 등이 이후 `lgb.train()`에서 뒤늦게 바뀌어
            거부되거나 다른 binning을 쓰지 않도록 학습 파라미터를 함께 넘긴다.
    returns:
        tuple[lgb.Dataset, np.ndarray, np.ndarray | None]: (construct()까지 끝난 Dataset, y, exposure)
    raises:
        ValueError: 이 날짜 구간에 실제 데이터가 하나도 없을 때(피처 마트가 아직
            안 쌓였을 수 있음) — `lgb.train()` 안에서 알아보기 힘든 에러로 죽기 전에
            여기서 먼저 걸러낸다.
    """
    prepass_columns = [label_col, "date"] + ([exposure_col] if exposure_col else [])
    prepass_df = s3_io.read_parquet(
        table_path, columns=prepass_columns, dates=dates, filters=filters, on_complete=on_prepass_complete
    )
    if prepass_df is None or prepass_df.empty:
        raise ValueError(f"학습 구간에 데이터가 없음: {table_path} dates={dates[:3]}...({len(dates)}개) — feature mart 확인 필요")

    groups = prepass_df.groupby("date", sort=False).groups
    row_counts = [len(groups[d]) if d in groups else 0 for d in dates]
    y = np.concatenate([prepass_df.loc[groups[d], label_col].to_numpy() for d in dates if d in groups]).astype(np.float64)
    exposure = None
    if exposure_col:
        exposure = np.concatenate(
            [prepass_df.loc[groups[d], exposure_col].to_numpy() for d in dates if d in groups]
        ).astype(np.float64)
    del prepass_df, groups

    sequences = [
        _DatePartitionSequence(
            table_path, d, feature_columns, station_dtype, filters, rc, cache, on_chunk_loaded=on_chunk_loaded
        )
        for d, rc in zip(dates, row_counts, strict=True)
        if rc > 0
    ]
    init_score = np.log(exposure) if exposure_col else None
    cat_idx = [feature_columns.index("station_no")]
    dataset = lgb.Dataset(
        data=sequences,
        label=y,
        init_score=init_score,
        reference=reference,
        feature_name=feature_columns,
        categorical_feature=cat_idx,
        params=dataset_params,
    )
    dataset.construct()
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
            chunk_df = _read_date_chunk(table_path, date_str, read_columns, filters)
        except FileNotFoundError:
            continue  # 이 날짜 파티션이 아예 없거나 filters 이후 0행 — 다음 날짜로

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
