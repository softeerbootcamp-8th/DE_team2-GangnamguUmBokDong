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
});
