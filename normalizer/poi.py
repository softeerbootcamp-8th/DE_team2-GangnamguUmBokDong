"""서울 주요 핫스팟(POI) Shapefile을 로드하고 위상 오류를 복구하여 EPSG:5179로 변환한다."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache


from pathlib import Path

# pyrefly: ignore [missing-import]
import shapefile
import shapely
# pyrefly: ignore [missing-import]
from pyproj import Transformer
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform as shapely_transform

DEFAULT_POI_SHP_PATH = str(Path(__file__).parent / "data" / "poi_areas" / "seoul_121_poi_areas.shp")

_TO_EPSG5179 = Transformer.from_crs("EPSG:4326", "EPSG:5179", always_xy=True)


@dataclass(frozen=True)
class PoiArea:
    """EPSG:5179로 변환 및 위상 복구가 완료된 POI 영역 지오메트리 정보."""

    area_cd: str
    area_nm: str
    geometry: BaseGeometry
    area_m2: float


def _select_largest_polygon(geom: BaseGeometry, area_cd: str) -> BaseGeometry:
    """복구 결과 지오메트리 중 면적이 가장 큰 Polygon을 선택한다.

    make_valid()로 자기교차된 폴리곤을 복구하면 주 영역 외에 미세한 파편들이 생성되므로
    실제 구역 본체에 해당하는 가장 큰 폴리곤만 선택한다.

    args:
        geom: BaseGeometry 객체
        area_cd: POI 영역 코드 
    returns:
        가장 큰 단일 Polygon 지오메트리
    raises:
        ValueError: Polygon 조각이 없거나 지원되지 않는 타입일 때
    """
    if geom.geom_type == "Polygon":
        return geom
    if geom.geom_type in ("MultiPolygon", "GeometryCollection"):
        polygons = [g for g in geom.geoms if g.geom_type == "Polygon"]
        if not polygons:
            raise ValueError(
                f"{area_cd}: make_valid 이후 Polygon 조각이 없음(geom_type={geom.geom_type})"
            )
        # 꼬임 복구 시 분리된 미세 파편 조각들을 제외하고 본체 구역만 선택
        return max(polygons, key=lambda p: p.area)
    raise ValueError(f"{area_cd}: 처리할 수 없는 geometry 타입 {geom.geom_type}")


def _fix_topology(geom: BaseGeometry, area_cd: str) -> BaseGeometry:
    """자기교차 등의 위상 오류가 있는 지오메트리를 유효한 폴리곤으로 복구한다.

    args:
        geom: 검사할 지오메트리
        area_cd: POI 영역 코드
    returns:
        위상이 복구된 유효한 지오메트리
    """
    if geom.is_valid:
        return geom
    fixed = shapely.make_valid(geom)
    return _select_largest_polygon(fixed, area_cd)


def _to_epsg5179(geom: BaseGeometry) -> BaseGeometry:
    """WGS84(EPSG:4326) 좌표계의 지오메트리를 UTM-K(EPSG:5179) 좌표계로 변환한다."""
    return shapely_transform(_TO_EPSG5179.transform, geom)


@lru_cache(maxsize=4)
def load_poi_areas(shp_path: str) -> tuple[PoiArea, ...]:
    """POI Shapefile을 읽어 위상 오류를 복구하고 EPSG:5179로 변환한 목록을 반환한다.

    파일 I/O 및 지오메트리 변환 연산 비용을 줄이기 위해 결과를 메모리에 캐싱(@lru_cache)하며,
    캐시 데이터의 외부 오염을 방지하기 위해 불변(tuple) 객체로 반환합니다.

    args:
        shp_path: Shapefile(.shp) 경로
    returns:
        PoiArea 객체 튜플 (결과는 캐싱됨)
    raises:
        ValueError: 레코드가 없거나 유효한 폴리곤이 없을 때
    """
    reader = shapefile.Reader(shp_path)
    areas: list[PoiArea] = []
    for shape_record in reader.iterShapeRecords():
        record = shape_record.record.as_dict()
        area_cd = record["AREA_CD"]
        area_nm = record["AREA_NM"]

        geom_wgs84 = shape(shape_record.shape.__geo_interface__)
        geom_wgs84 = _fix_topology(geom_wgs84, area_cd)
        geom_5179 = _to_epsg5179(geom_wgs84)

        areas.append(
            PoiArea(area_cd=area_cd, area_nm=area_nm, geometry=geom_5179, area_m2=geom_5179.area)
        )

    if not areas:
        raise ValueError(f"POI shapefile에 레코드가 없음: {shp_path}")

    # 캐시 오염 방지를 위해 불변 튜플로 반환
    return tuple(areas)
