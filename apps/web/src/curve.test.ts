import { describe, expect, it } from "vitest";
import { pairedMonotonePaths } from "./curve";

function controlYs(path: string): number[] {
  return path.split(" C ").slice(1).flatMap((segment) => {
    const [firstControl, secondControl] = segment.split(", ");
    return [
      Number(firstControl.split(" ")[1]),
      Number(secondControl.split(" ")[1]),
    ];
  });
}

describe("pairedMonotonePaths", () => {
  it("같은 방향의 두 곡선이 제어점 사이에서도 최소 간격을 유지한다", () => {
    const [upperPath, lowerPath] = pairedMonotonePaths(
      [0, 1, 2],
      [0, 10, 11],
      [5, 15, 30],
      5,
    );
    const upperControls = controlYs(upperPath);
    const lowerControls = controlYs(lowerPath);

    upperControls.forEach((upperY, index) => {
      expect(lowerControls[index] - upperY).toBeGreaterThanOrEqual(5);
    });
  });
});
