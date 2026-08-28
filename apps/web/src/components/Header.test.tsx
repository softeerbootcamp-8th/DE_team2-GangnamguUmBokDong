// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ServingHealthResponse } from "../api";
import { Header } from "./Header";

const HEALTH: ServingHealthResponse = {
  overall: "degraded",
  operational_base_dttm: "2026-08-20T00:00:00Z",
  checked_at: "2026-08-20T00:05:00Z",
  can_dispatch_new_routes: false,
  components: {
    stock: { state: "ready", data_dttm: "2026-08-20T00:00:00Z", age_minutes: 5, reason: "fresh" },
    demand: { state: "ready", data_dttm: "2026-08-20T00:00:00Z", age_minutes: 5, reason: "fresh" },
    urgency: { state: "stale", data_dttm: "2026-08-19T23:50:00Z", age_minutes: 15, reason: "publication_stale" },
    routes: { state: "misaligned", data_dttm: "2026-08-19T23:50:00Z", age_minutes: 15, reason: "operational_anchor_mismatch" },
    weather: {
      state: "stale",
      data_dttm: "2026-08-20T00:00:00Z",
      source_dttm: "2026-08-19T20:00:00Z",
      age_minutes: 5,
      reason: "weather_issue_stale",
    },
    events: { state: "missing", data_dttm: null, age_minutes: null, reason: "not_published" },
    regions: { state: "ready", data_dttm: "2026-08-19T00:00:00Z", age_minutes: 1445, reason: "fresh" },
  },
};

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-08-20T00:05:00Z"));
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

function renderHeader(overrides: Partial<React.ComponentProps<typeof Header>> = {}) {
  render(
    <Header
      regions={[]}
      selectedRegion="all"
      stationsUpdatedAt={new Date("2026-08-20T00:05:00Z")}
      servingHealth={HEALTH}
      servingHealthError={false}
      onRegionChange={vi.fn()}
      {...overrides}
    />,
  );
}

describe("Header serving health", () => {
  it("권역 목록에서 강북과 강남 관리소를 선택할 수 있다", () => {
    const onRegionChange = vi.fn();
    renderHeader({ onRegionChange });

    const select = screen.getByRole("combobox", { name: "권역 선택" });
    expect(screen.getByRole("option", { name: "강북" })).not.toBeNull();
    expect(screen.getByRole("option", { name: "강남" })).not.toBeNull();
    fireEvent.change(select, { target: { value: "강남" } });

    expect(onRegionChange).toHaveBeenCalledWith("강남");
  });

  it("기준 시각 정보창에 모든 데이터 상태를 세로로 표시한다", () => {
    renderHeader();

    expect(screen.getByText(/^조회 시각 /)).not.toBeNull();
    expect(screen.getByText("일부 지연")).not.toBeNull();
    expect(screen.getByRole("button", { name: "기준 시각 및 데이터 상태 설명" })).not.toBeNull();
    expect(screen.getByRole("tooltip", { name: "데이터 상태 상세" })).not.toBeNull();
    ["대여소·재고", "수요예측", "대여소 우선순위", "작업 추천", "날씨 예보", "행사", "권역 설정"]
      .forEach((label) => expect(screen.getByText(label)).not.toBeNull());
    expect(screen.getByText("기준 불일치")).not.toBeNull();
    expect(screen.getByText("미게시")).not.toBeNull();
    ["항목", "상태", "기준 시각"]
      .forEach((heading) => expect(screen.getByText(heading)).not.toBeNull());
    expect(screen.getByText(/^기상청 발표 8\./)).not.toBeNull();
  });

  it("작은 화면에서도 기준 시각 정보창을 viewport 안에 배치한다", () => {
    Object.defineProperty(window, "innerWidth", { configurable: true, value: 240 });
    Object.defineProperty(window, "innerHeight", { configurable: true, value: 500 });
    renderHeader();
    const trigger = screen.getByRole("button", { name: "기준 시각 및 데이터 상태 설명" });
    vi.spyOn(trigger, "getBoundingClientRect").mockReturnValue({
      bottom: 24,
      height: 14,
      left: 4,
      right: 18,
      top: 10,
      width: 14,
      x: 4,
      y: 10,
      toJSON: () => ({}),
    });

    fireEvent.mouseEnter(trigger);

    const tooltip = screen.getByRole("tooltip", { name: "데이터 상태 상세" });
    expect(tooltip.style.left).toBe("8px");
    expect(tooltip.classList.contains("is-visible")).toBe(true);
  });

  it("날씨 예보 horizon이 불완전하면 예보 구간 누락으로 표시한다", () => {
    renderHeader({
      servingHealth: {
        ...HEALTH,
        components: {
          ...HEALTH.components,
          weather: {
            ...HEALTH.components.weather,
            state: "misaligned",
            reason: "weather_horizon_incomplete",
          },
        },
      },
    });

    expect(screen.getByText("구간 누락")).not.toBeNull();
  });

  it("상태 조회 실패 뒤 마지막 기준 시각과 상세는 유지하고 연결 끊김을 표시한다", () => {
    renderHeader({ servingHealthError: true });

    expect(screen.getByText(/^기준 시각 /).textContent).not.toBe("기준 시각 -");
    expect(screen.getByText("연결 끊김")).not.toBeNull();
    expect(screen.getByText("상태 조회 실패 · 마지막 정상 화면을 유지합니다.")).not.toBeNull();
    expect(screen.getByText("대여소·재고")).not.toBeNull();
  });

  it("최초 상태 확인 전에는 확인 중으로 표시한다", () => {
    renderHeader({ servingHealth: null, stationsUpdatedAt: null });

    expect(screen.getByText("기준 시각 -")).not.toBeNull();
    expect(screen.getAllByText("확인 중").length).toBeGreaterThan(1);
  });
});
