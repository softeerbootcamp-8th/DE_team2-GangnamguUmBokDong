import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "./api";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("api", () => {
  it("HTTP 오류의 status와 detail을 보존한다", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ detail: "forecast_not_available" }), {
        status: 404,
        headers: { "content-type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const request = api.forecast("ST-1");

    await expect(request).rejects.toEqual(
      expect.objectContaining({
        name: "ApiError",
        status: 404,
        detail: "forecast_not_available",
      }),
    );
    expect(fetchMock).toHaveBeenCalledWith("http://localhost:8000/stations/ST-1/forecast");
  });

  it("날씨를 고정된 12시간 계약으로 요청한다", async () => {
    const response = { sta_id: "ST-1", points: [] };
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(response), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.weather("ST-1")).resolves.toEqual(response);
    expect(fetchMock).toHaveBeenCalledWith("http://localhost:8000/stations/ST-1/weather?hours=12");
  });

  it("작업 목록 필터를 query string으로 직렬화한다", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("[]", { status: 200, headers: { "Content-Type": "application/json" } }),
    );

    await expect(api.routes({
      region: "영남",
      status: "dispatched",
      closedWithinMinutes: 60,
    })).resolves.toEqual([]);

    expect(fetchMock).toHaveBeenCalledWith(
      "http://localhost:8000/routes?region=%EC%98%81%EB%82%A8&status=dispatched&closed_within_minutes=60&limit=500&offset=0",
    );
  });

  it("작업 승인 요청은 POST를 사용한다", async () => {
    const response = {
      route_id: "route-1",
      region: "영남",
      status: "dispatched",
      proposed_at: "2026-08-20T00:00:00Z",
      dispatched_at: "2026-08-20T00:01:00Z",
      completed_at: null,
      cancelled_at: null,
      stops: [],
    };
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(response), { status: 200, headers: { "Content-Type": "application/json" } }),
    );

    await expect(api.dispatchRoute("route-1")).resolves.toEqual(response);

    expect(fetchMock).toHaveBeenCalledWith("http://localhost:8000/routes/route-1/dispatch", { method: "POST" });
  });
});
