"""POI(121장소) shapefile 로딩, 위상 오류(POI070) 복구, 결과 캐싱.

shapefile은 이미 EPSG:4326(WGS84)이다. AREA_CD/AREA_NM/geometry가 shapefile
자체에 있어 별도 목록.xlsx와의 조인이 필요 없다(spec 확인 완료).

실측으로 발견된 버그: POI070("쌍문역")이 shapely가 TopologyException을 던질
정도로 자기교차한다. is_valid 체크 후 shapely.make_valid로 복구하고, 복구
결과가 단일 Polygon이 아니면(MultiPolygon/GeometryCollection) 면적이 가장
큰 Polygon 조각을 취한다. Polygon 조각이 하나도 없으면 명확히 실패시킨다.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

import shapefile
import shapely
from pyproj import Transformer
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform as shapely_transform

DEFAULT_POI_SHP_PATH = "data/poi_areas/seoul_121_poi_areas.shp"

_TO_EPSG5179 = Transformer.from_crs("EPSG:4326", "EPSG:5179", always_xy=True)


@dataclass(frozen=True)
class PoiArea:
    """shapefile에서 로딩한 POI 하나(EPSG:5179로 변환·위상 오류 복구 완료)."""

    area_cd: str
    area_nm: str
    geometry: BaseGeometry
    area_m2: float


def _select_largest_polygon(geom: BaseGeometry, area_cd: str) -> BaseGeometry:
    """make_valid 등의 결과에서 면적이 가장 큰 Polygon 조각을 고른다.

    Args:
        geom: Polygon, MultiPolygon, 또는 GeometryCollection.
        area_cd: 에러 메시지에 쓸 POI 식별자.

    Raises:
        ValueError: Polygon 조각이 하나도 없거나 처리할 수 없는 타입일 때.
    """
    if geom.geom_type == "Polygon":
        return geom
    if geom.geom_type in ("MultiPolygon", "GeometryCollection"):
        polygons = [g for g in geom.geoms if g.geom_type == "Polygon"]
        if not polygons:
            raise ValueError(
                f"{area_cd}: make_valid 이후 Polygon 조각이 없음(geom_type={geom.geom_type})"
            )
        return max(polygons, key=lambda p: p.area)
    raise ValueError(f"{area_cd}: 처리할 수 없는 geometry 타입 {geom.geom_type}")


def _fix_topology(geom: BaseGeometry, area_cd: str) -> BaseGeometry:
    """자기교차 등 위상 오류가 있으면 make_valid로 복구한다."""
    if geom.is_valid:
        return geom
    fixed = shapely.make_valid(geom)
    return _select_largest_polygon(fixed, area_cd)


def _to_epsg5179(geom: BaseGeometry) -> BaseGeometry:
    return shapely_transform(_TO_EPSG5179.transform, geom)


@lru_cache(maxsize=4)
def load_poi_areas(shp_path: str) -> tuple[PoiArea, ...]:
    """POI shapefile을 읽어 위상 오류를 복구하고 EPSG:5179로 변환한 뒤 캐싱한다.

    Args:
        shp_path: `.shp` 파일 경로(`.dbf`/`.shx`/`.prj`/`.cpg`가 같은 디렉터리에 있어야 함).

    Returns:
        PoiArea 튜플(같은 shp_path로 다시 호출하면 동일 객체를 반환).

    Raises:
        ValueError: 위상 오류 복구 후에도 Polygon이 없거나, shapefile에
            레코드가 하나도 없을 때.
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

    return tuple(areas)
