// @vitest-environment jsdom

import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Header } from "./Header";

const apiMock = vi.hoisted(() => ({ status: vi.fn() }));

vi.mock("../api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../api")>();
  return { ...actual, api: apiMock };
});

beforeEach(() => {
  vi.useFakeTimers();
  vi.clearAllMocks();
  apiMock.status.mockReset().mockResolvedValue({ base_dttm: "2026-08-20T00:00:00Z" });
});

afterEach(() => {
  cleanup();
  vi.clearAllTimers();
  vi.useRealTimers();
});

describe("Header status polling", () => {
  it("대여소 polling 성공 시각을 조회 시각으로 표시한다", () => {
    render(
      <Header
        regions={[]}
        selectedRegion="all"
        stationsUpdatedAt={new Date("2026-08-20T00:05:00Z")}
        onRegionChange={vi.fn()}
      />,
    );

    expect(screen.getByText(/^조회 시각 /)).not.toBeNull();
    expect(screen.getByRole("button", { name: "현재 시각 설명" })).not.toBeNull();
    expect(screen.getByRole("button", { name: "조회 시각 설명" })).not.toBeNull();
    expect(screen.getByRole("button", { name: "기준 시각 설명" })).not.toBeNull();
    expect(screen.getByRole("tooltip", { name: /대여소 정보를 API에서 성공적으로 조회/ })).not.toBeNull();
  });

  it("status 실패 뒤 이전 성공 시각을 지운다", async () => {
    apiMock.status
      .mockResolvedValueOnce({ base_dttm: "2026-08-20T00:00:00Z" })
      .mockRejectedValueOnce(new Error("network unavailable"));
    render(<Header regions={[]} selectedRegion="all" stationsUpdatedAt={null} onRegionChange={vi.fn()} />);
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByText(/^기준 시각 /).textContent).not.toBe("기준 시각 -");

    await act(async () => {
      vi.advanceTimersByTime(30_000);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByText("기준 시각 갱신 실패")).not.toBeNull();
  });

  it("초기 loading과 실패를 구분하고 느린 이전 성공을 무시한다", async () => {
    let resolveOld!: (value: { base_dttm: string }) => void;
    const oldRequest = new Promise<{ base_dttm: string }>((resolve) => {
      resolveOld = resolve;
    });
    apiMock.status.mockReturnValueOnce(oldRequest).mockRejectedValueOnce(new Error("network unavailable"));
    render(<Header regions={[]} selectedRegion="all" stationsUpdatedAt={null} onRegionChange={vi.fn()} />);
    expect(screen.getByText("기준 시각 -")).not.toBeNull();

    await act(async () => {
      vi.advanceTimersByTime(30_000);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByText("기준 시각 갱신 실패")).not.toBeNull();

    resolveOld({ base_dttm: "2026-08-20T00:00:00Z" });
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByText("기준 시각 갱신 실패")).not.toBeNull();
  });
});
