// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { updateRoutesWithMotion } from "./routeCardMotion";

const ROUTE_ID = "11111111-1111-4111-8111-111111111111";

function renderMotionFixture(): {
  candidateList: HTMLElement;
  card: HTMLElement;
  operationList: HTMLElement;
} {
  document.body.innerHTML = `
    <div class="route-workspace">
      <section data-column="candidate">
        <ul class="route-column-list"></ul>
      </section>
      <section data-column="operation">
        <ul class="route-column-list"></ul>
      </section>
    </div>
  `;
  const candidateList = document.querySelector<HTMLElement>('[data-column="candidate"] ul')!;
  const operationList = document.querySelector<HTMLElement>('[data-column="operation"] ul')!;
  const card = document.createElement("article");
  card.className = "route-card";
  card.dataset.routeId = ROUTE_ID;
  card.innerHTML = "<button type=\"button\">작업</button>";
  candidateList.append(card);
  return { candidateList, card, operationList };
}

function mockElementAnimation() {
  const animate = vi.fn((..._args: unknown[]) => ({ finished: Promise.resolve() }));
  const scrollTo = vi.fn();
  Object.defineProperty(HTMLElement.prototype, "animate", {
    configurable: true,
    value: animate,
  });
  Object.defineProperty(HTMLElement.prototype, "scrollTo", {
    configurable: true,
    value: scrollTo,
  });
  return { animate, scrollTo };
}

afterEach(() => {
  document.body.innerHTML = "";
  Reflect.deleteProperty(HTMLElement.prototype, "animate");
  Reflect.deleteProperty(HTMLElement.prototype, "scrollTo");
  vi.restoreAllMocks();
});

describe("route card motion", () => {
  it("승인 카드를 작업 후보에서 작업 현황으로 가로 이동한다", async () => {
    const { card, operationList } = renderMotionFixture();
    const { animate, scrollTo } = mockElementAnimation();
    vi.spyOn(HTMLElement.prototype, "getBoundingClientRect")
      .mockImplementation(function (this: HTMLElement) {
        if (this.classList.contains("route-workspace")) return new DOMRect(0, 0, 1000, 500);
        const isCandidate = this.closest('[data-column="candidate"]') !== null;
        if (this.classList.contains("route-column-list")) {
          return new DOMRect(isCandidate ? 10 : 510, 40, 480, 440);
        }
        if (this.matches("[data-route-id]")) {
          return new DOMRect(isCandidate ? 20 : 520, 80, 450, 92);
        }
        return new DOMRect();
      });

    await updateRoutesWithMotion(ROUTE_ID, () => operationList.append(card));

    expect(scrollTo).toHaveBeenCalledWith({ behavior: "smooth", top: 0 });
    const keyframes = animate.mock.calls[0][0] as Keyframe[];
    expect(keyframes[1].transform).toContain("translate(500px, 0px)");
    expect(document.querySelector(".route-card-motion-ghost")).toBeNull();
  });

  it("취소 카드를 작업 중 위치로 올리며 목록을 위로 스크롤한다", async () => {
    const { card, operationList } = renderMotionFixture();
    operationList.append(card);
    card.dataset.position = "cancelled";
    Object.defineProperties(operationList, {
      clientHeight: { configurable: true, value: 400 },
      scrollHeight: { configurable: true, value: 800 },
      scrollTop: { configurable: true, value: 350, writable: true },
    });
    const { animate, scrollTo } = mockElementAnimation();
    vi.spyOn(HTMLElement.prototype, "getBoundingClientRect")
      .mockImplementation(function (this: HTMLElement) {
        if (this.classList.contains("route-workspace")) return new DOMRect(0, 0, 1000, 500);
        if (this.classList.contains("route-column-list")) return new DOMRect(510, 40, 480, 400);
        if (this.matches("[data-route-id]")) {
          return new DOMRect(520, this.dataset.position === "cancelled" ? 330 : -300, 450, 92);
        }
        return new DOMRect();
      });

    await updateRoutesWithMotion(ROUTE_ID, () => {
      card.dataset.position = "dispatched";
      operationList.prepend(card);
    });

    expect(scrollTo).toHaveBeenCalledWith({ behavior: "smooth", top: 0 });
    const keyframes = animate.mock.calls[0][0] as Keyframe[];
    expect(keyframes[1].transform).toContain("translate(0px, -280px)");
    expect(document.querySelector(".route-card-motion-ghost")).toBeNull();
  });
});
