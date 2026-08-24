import { describe, expect, it } from "vitest";
import { DETAIL_CONTENT_FIT_HEIGHT_PX, detailPanelDefaultHeight } from "./panelLayout";

describe("detailPanelDefaultHeight", () => {
  it("정보를 모두 보여주는 높이보다 화면 절반이 작으면 절반만 사용한다", () => {
    expect(detailPanelDefaultHeight(600)).toBe(300);
  });

  it("화면이 충분하면 정보에 필요한 높이까지만 사용한다", () => {
    expect(detailPanelDefaultHeight(1_000)).toBe(DETAIL_CONTENT_FIT_HEIGHT_PX);
  });

  it("측정 전이거나 잘못된 높이에는 크기를 적용하지 않는다", () => {
    expect(detailPanelDefaultHeight(0)).toBe(0);
    expect(detailPanelDefaultHeight(Number.NaN)).toBe(0);
  });
});
