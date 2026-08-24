/** 하단 상세 영역에서 경로·그래프·대여소 정보를 한 번에 읽기 좋은 높이다. */
export const DETAIL_CONTENT_FIT_HEIGHT_PX = 380;

/** 사용자 조절 전 적용할 하단 상세 영역의 기본 높이를 계산한다. */
export function detailPanelDefaultHeight(groupHeight: number): number {
  if (!Number.isFinite(groupHeight) || groupHeight <= 0) return 0;
  return Math.min(DETAIL_CONTENT_FIT_HEIGHT_PX, groupHeight / 2);
}
