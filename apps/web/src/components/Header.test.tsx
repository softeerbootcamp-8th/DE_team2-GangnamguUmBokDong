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
});

afterEach(() => {
  cleanup();
  vi.clearAllTimers();
  vi.useRealTimers();
});

describe("Header status polling", () => {
  it("status 실패 뒤 이전 성공 시각을 지운다", async () => {
    apiMock.status
      .mockResolvedValueOnce({ base_dttm: "2026-08-20T00:00:00Z" })
      .mockRejectedValueOnce(new Error("network unavailable"));
    render(<Header regions={[]} selectedRegion="all" stationsUpdatedAt={null} onRegionChange={vi.fn()} />);
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByText(/^예측 시각 /).textContent).not.toBe("예측 시각 -");

    await act(async () => {
      vi.advanceTimersByTime(30_000);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByText("예측 시각 갱신 실패")).not.toBeNull();
  });

  it("초기 loading과 실패를 구분하고 느린 이전 성공을 무시한다", async () => {
    let resolveOld!: (value: { base_dttm: string }) => void;
    const oldRequest = new Promise<{ base_dttm: string }>((resolve) => {
      resolveOld = resolve;
    });
    apiMock.status.mockReturnValueOnce(oldRequest).mockRejectedValueOnce(new Error("network unavailable"));
    render(<Header regions={[]} selectedRegion="all" stationsUpdatedAt={null} onRegionChange={vi.fn()} />);
    expect(screen.getByText("예측 시각 -")).not.toBeNull();

    await act(async () => {
      vi.advanceTimersByTime(30_000);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByText("예측 시각 갱신 실패")).not.toBeNull();

    resolveOld({ base_dttm: "2026-08-20T00:00:00Z" });
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByText("예측 시각 갱신 실패")).not.toBeNull();
  });
});
