// @vitest-environment jsdom

import { cleanup, render, screen } from "@testing-library/react";
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
  it("기준 시각 정보창에 모든 데이터 상태를 세로로 표시한다", () => {
    renderHeader();

    expect(screen.getByText(/^조회 시각 /)).not.toBeNull();
    expect(screen.getAllByText("일부 지연")).toHaveLength(2);
    expect(screen.getByRole("button", { name: "기준 시각 및 데이터 상태 설명" })).not.toBeNull();
    expect(screen.getByRole("tooltip", { name: "데이터 상태 상세" })).not.toBeNull();
    ["대여소·재고", "수요예측", "대여소 우선순위", "작업 추천", "날씨", "행사", "권역 설정"]
      .forEach((label) => expect(screen.getByText(label)).not.toBeNull());
    expect(screen.getByText("기준 불일치")).not.toBeNull();
    expect(screen.getByText("미게시")).not.toBeNull();
    expect(screen.getByText("원본 지연")).not.toBeNull();
    expect(screen.getByText(/^게시 /)).not.toBeNull();
    expect(screen.getByText(/^원본 8\./)).not.toBeNull();
  });

  it("상태 조회 실패 뒤 마지막 기준 시각과 상세는 유지하고 연결 끊김을 표시한다", () => {
    renderHeader({ servingHealthError: true });

    expect(screen.getByText(/^기준 시각 /).textContent).not.toBe("기준 시각 -");
    expect(screen.getAllByText("연결 끊김")).toHaveLength(2);
    expect(screen.getByText("상태 조회 실패 · 마지막 정상 화면을 유지합니다.")).not.toBeNull();
    expect(screen.getByText("대여소·재고")).not.toBeNull();
  });

  it("최초 상태 확인 전에는 확인 중으로 표시한다", () => {
    renderHeader({ servingHealth: null, stationsUpdatedAt: null });

    expect(screen.getByText("기준 시각 -")).not.toBeNull();
    expect(screen.getAllByText("확인 중").length).toBeGreaterThan(1);
  });
});
