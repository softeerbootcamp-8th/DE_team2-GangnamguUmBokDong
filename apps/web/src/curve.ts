/**
 * 점들을 이어서 SVG path를 그린다. 직선(L) 대신 monotone cubic interpolation을
 * 쓰면, 인접한 두 점의 값 범위를 벗어나는 구간(오버슈트) 없이 부드럽게 이어진다.
 * natural cubic spline이나 Catmull-Rom은 두 점 사이에서 실제 값보다 더 튀어
 * 오르내릴 수 있는데, 이 그래프들은 1시간 단위 예측치를 보여주는 거라 그 사이를
 * 실제로 예측하지 않은 값처럼 그리면 안 된다(정원 기준선을 잘못 넘는 것처럼
 * 보이는 등). 접선을 조화평균(harmonic mean)으로 잡는 방식(Fritsch-Carlson류)은
 * 이 오버슈트가 구조적으로 나지 않는다.
 */
export function monotonePath(xs: number[], ys: number[]): string {
  const n = xs.length;
  if (n === 0) return "";
  if (n === 1) return `M ${xs[0]} ${ys[0]}`;

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

  let path = `M ${xs[0]} ${ys[0]}`;
  for (let i = 0; i < n - 1; i++) {
    const cp1x = xs[i] + dx[i] / 3;
    const cp1y = ys[i] + (tangent[i] * dx[i]) / 3;
    const cp2x = xs[i + 1] - dx[i] / 3;
    const cp2y = ys[i + 1] - (tangent[i + 1] * dx[i]) / 3;
    path += ` C ${cp1x} ${cp1y}, ${cp2x} ${cp2y}, ${xs[i + 1]} ${ys[i + 1]}`;
  }
  return path;
}
