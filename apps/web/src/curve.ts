/**
 * 점들을 이어서 SVG path를 그린다. 직선(L) 대신 monotone cubic interpolation을
 * 쓰면, 인접한 두 점의 값 범위를 벗어나는 구간(오버슈트) 없이 부드럽게 이어진다.
 * natural cubic spline이나 Catmull-Rom은 두 점 사이에서 실제 값보다 더 튀어
 * 오르내릴 수 있는데, 이 그래프들은 1시간 단위 예측치를 보여주는 거라 그 사이를
 * 실제로 예측하지 않은 값처럼 그리면 안 된다(정원 기준선을 잘못 넘는 것처럼
 * 보이는 등). 접선을 조화평균(harmonic mean)으로 잡는 방식(Fritsch-Carlson류)은
 * 이 오버슈트가 구조적으로 나지 않는다.
 */
interface CubicSegment {
  cp1x: number;
  cp1y: number;
  cp2x: number;
  cp2y: number;
  x: number;
  y: number;
}

function monotoneSegments(xs: number[], ys: number[]): CubicSegment[] {
  const n = xs.length;
  if (n < 2) return [];

  const dx: number[] = [];
  const slope: number[] = [];
  for (let i = 0; i < n - 1; i++) {
    dx.push(xs[i + 1] - xs[i]);
    slope.push((ys[i + 1] - ys[i]) / dx[i]);
  }

  const tangent: number[] = new Array(n);
  tangent[0] = slope[0];
  tangent[n - 1] = slope[n - 2];
  for (let i = 1; i < n - 1; i++) {
    const s0 = slope[i - 1];
    const s1 = slope[i];
    // 방향이 바뀌는 지점(극값)에서는 접선을 0으로 둬서 그 지점 앞뒤로 오버슈트가
    // 안 생기게 한다. 같은 방향이면 조화평균으로 접선을 잡는다.
    tangent[i] = s0 * s1 <= 0 ? 0 : (2 * s0 * s1) / (s0 + s1);
  }

  const segments: CubicSegment[] = [];
  for (let i = 0; i < n - 1; i++) {
    const cp1x = xs[i] + dx[i] / 3;
    const cp1y = ys[i] + (tangent[i] * dx[i]) / 3;
    const cp2x = xs[i + 1] - dx[i] / 3;
    const cp2y = ys[i + 1] - (tangent[i + 1] * dx[i]) / 3;
    segments.push({ cp1x, cp1y, cp2x, cp2y, x: xs[i + 1], y: ys[i + 1] });
  }
  return segments;
}

function serializePath(startX: number, startY: number, segments: CubicSegment[]): string {
  return segments.reduce(
    (path, segment) => `${path} C ${segment.cp1x} ${segment.cp1y}, ${segment.cp2x} ${segment.cp2y}, ${segment.x} ${segment.y}`,
    `M ${startX} ${startY}`,
  );
}

export function monotonePath(xs: number[], ys: number[]): string {
  const n = xs.length;
  if (n === 0) return "";
  if (n === 1) return `M ${xs[0]} ${ys[0]}`;

  const path = serializePath(xs[0], ys[0], monotoneSegments(xs, ys));
  return path;
}

/** 같은 순서를 유지하는 두 곡선이 점 사이에서도 최소 간격을 갖도록 그린다. */
export function pairedMonotonePaths(
  xs: number[],
  firstYs: number[],
  secondYs: number[],
  minimumDistance: number,
): [string, string] {
  if (xs.length === 0) return ["", ""];
  if (xs.length === 1) {
    return [`M ${xs[0]} ${firstYs[0]}`, `M ${xs[0]} ${secondYs[0]}`];
  }

  const firstSegments = monotoneSegments(xs, firstYs);
  const secondSegments = monotoneSegments(xs, secondYs);

  const separateControlPair = (
    firstValue: number,
    secondValue: number,
    firstIsAbove: boolean,
  ): [number, number] => {
    if (Math.abs(firstValue - secondValue) >= minimumDistance) {
      return [firstValue, secondValue];
    }
    const midpoint = (firstValue + secondValue) / 2;
    const halfDistance = minimumDistance / 2;
    return firstIsAbove
      ? [midpoint - halfDistance, midpoint + halfDistance]
      : [midpoint + halfDistance, midpoint - halfDistance];
  };

  firstSegments.forEach((firstSegment, index) => {
    const secondSegment = secondSegments[index];
    const startDifference = firstYs[index] - secondYs[index];
    const endDifference = firstYs[index + 1] - secondYs[index + 1];

    // 양 끝의 상하관계가 같을 때만 제어점도 같은 간격으로 제한한다. 관계가
    // 뒤집히는 구간은 실제 교차이므로 원래 곡선을 유지한다.
    if (startDifference * endDifference <= 0) return;
    const firstIsAbove = startDifference < 0;
    [firstSegment.cp1y, secondSegment.cp1y] = separateControlPair(
      firstSegment.cp1y,
      secondSegment.cp1y,
      firstIsAbove,
    );
    [firstSegment.cp2y, secondSegment.cp2y] = separateControlPair(
      firstSegment.cp2y,
      secondSegment.cp2y,
      firstIsAbove,
    );
  });

  return [
    serializePath(xs[0], firstYs[0], firstSegments),
    serializePath(xs[0], secondYs[0], secondSegments),
  ];
}
