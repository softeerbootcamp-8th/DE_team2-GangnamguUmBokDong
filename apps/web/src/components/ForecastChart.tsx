import { useState } from "react";
import type { ForecastPoint } from "../api";
import { pairedMonotonePaths } from "../curve";
import { ACTION_LABEL, formatIsoTime } from "../format";

interface Props {
  baseDttm: string;
  points: ForecastPoint[];
}

const WIDTH = 600;
const HEIGHT = 220;
const MARGIN = { top: 16, right: 16, bottom: 24, left: 32 };
const PLOT_WIDTH = WIDTH - MARGIN.left - MARGIN.right;
const PLOT_HEIGHT = HEIGHT - MARGIN.top - MARGIN.bottom;
const X_TICK_INTERVAL_MS = 3 * 60 * 60 * 1000;
const FORECAST_INTERVAL_MS = 60 * 60 * 1000;
const SERIES_STROKE_WIDTH = 2;
const OVERLAP_CENTER_DISTANCE = SERIES_STROKE_WIDTH + 2;

// 대여가 늘면 재고가 부족해지고(공급필요, 빨강), 반납이 늘면 재고가 넘친다
// (회수필요, 파랑) — 지도 마커의 방향별 색과 같은 의미로 맞춘다.
const SERIES = [
  { key: "predicted_rent_cnt" as const, label: "시간당 예측 대여량", color: "var(--diverging-red)" },
  { key: "predicted_return_cnt" as const, label: "시간당 예측 반납량", color: "var(--diverging-blue)" },
];

function formatTime(iso: string): string {
  return formatIsoTime(iso, { hour: "2-digit", minute: "2-digit" });
}

function tickTimes(startMs: number, endMs: number): number[] {
  const ticks: number[] = [];
  for (let timestamp = startMs; timestamp <= endMs; timestamp += X_TICK_INTERVAL_MS) {
    ticks.push(timestamp);
  }
  return ticks;
}

export function ForecastChart({ baseDttm, points }: Props) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  if (points.length === 0) {
    return <p className="empty-state">예측 데이터가 없습니다.</p>;
  }

  const maxY = Math.max(1, ...points.flatMap((p) => [p.predicted_rent_cnt, p.predicted_return_cnt])) * 1.2;
  const startMs = Date.parse(baseDttm);
  const endMs = Date.parse(points[points.length - 1].predicted_dttm);
  const timeRangeMs = Math.max(1, endMs - startMs);
  const xAtTime = (time: string | number) => {
    const timestamp = typeof time === "number" ? time : Date.parse(time);
    return MARGIN.left + ((timestamp - startMs) / timeRangeMs) * PLOT_WIDTH;
  };
  // 수요값은 predicted_dttm 순간의 값이 아니라 그 시각에 끝나는 1시간 구간의
  // 합계다. 각 구간의 중앙에 점을 두면 시점값으로 오해하지 않으면서 추세를
  // 선으로 연결할 수 있다.
  const intervalMidpointMs = (i: number) => (
    Date.parse(points[i].predicted_dttm) - FORECAST_INTERVAL_MS / 2
  );
  const xAt = (i: number) => xAtTime(intervalMidpointMs(i));
  const yAt = (v: number) => MARGIN.top + (1 - v / maxY) * PLOT_HEIGHT;
  const rentIsAboveForOverlap = (index: number) => {
    const currentRentY = yAt(points[index].predicted_rent_cnt);
    const currentReturnY = yAt(points[index].predicted_return_cnt);
    if (currentRentY !== currentReturnY) {
      return currentRentY < currentReturnY;
    }

    for (let previous = index - 1; previous >= 0; previous -= 1) {
      const rentY = yAt(points[previous].predicted_rent_cnt);
      const returnY = yAt(points[previous].predicted_return_cnt);
      if (rentY !== returnY) {
        return rentY < returnY;
      }
    }

    for (let next = index + 1; next < points.length; next += 1) {
      const rentY = yAt(points[next].predicted_rent_cnt);
      const returnY = yAt(points[next].predicted_return_cnt);
      if (rentY !== returnY) {
        return rentY < returnY;
      }
    }

    return true;
  };
  const visualYAt = (key: (typeof SERIES)[number]["key"], index: number) => {
    const point = points[index];
    const rentY = yAt(point.predicted_rent_cnt);
    const returnY = yAt(point.predicted_return_cnt);
    const distance = Math.abs(rentY - returnY);

    if (distance >= OVERLAP_CENTER_DISTANCE) {
      return key === "predicted_rent_cnt" ? rentY : returnY;
    }

    // 실제 값과 툴팁은 그대로 두고, 두 선의 중심이 4px보다 가까운 구간은 화면
    // 중심을 4px 벌려 2px 여백을 만든다. 점에서 이미 겹친 뒤에만 보정하면 같은
    // 기울기의 대각선 구간이 다시 붙어 보이므로 근접한 시점부터 함께 보정한다.
    // 현재 값의 상하관계를 우선하고 값이 완전히 같을 때만 직전 관계를 이어받는다.
    const midpoint = (rentY + returnY) / 2;
    const halfDistance = OVERLAP_CENTER_DISTANCE / 2;
    const rentAboveReturn = rentIsAboveForOverlap(index);
    if (key === "predicted_rent_cnt") {
      return midpoint + (rentAboveReturn ? -halfDistance : halfDistance);
    }
    return midpoint + (rentAboveReturn ? halfDistance : -halfDistance);
  };
  const yTicks = [0, Math.round(maxY / 2), Math.round(maxY)];
  const xTicks = tickTimes(startMs, endMs);
  const criticalIndex = points.findIndex((p) => p.action_type !== "normal");
  const criticalPoint = criticalIndex >= 0 ? points[criticalIndex] : null;
  // 이 선의 역할은 "언제 문제가 생기는지" 경고이지, 방향(공급/회수) 표시가 아니다
  // — 방향은 옆에 붙는 라벨 텍스트가 이미 말해준다. 데이터 선(빨강/파랑)과 같은
  // 색 계열을 쓰면 구분이 안 된다는 의견이 있어서, 경고를 뜻하는 색으로 뺐다.
  const criticalColor = "var(--status-warning)";
  const criticalAnchor = criticalIndex <= 1 ? "start" : criticalIndex >= points.length - 2 ? "end" : "middle";
  const pointXs = points.map((_, i) => xAt(i));
  const chartXs = [MARGIN.left, ...pointXs, WIDTH - MARGIN.right];
  const seriesPointYs = SERIES.map((series) => (
    points.map((_, index) => visualYAt(series.key, index))
  ));
  const chartYs = seriesPointYs.map((pointYs) => (
    [pointYs[0], ...pointYs, pointYs[pointYs.length - 1]]
  ));
  const seriesPaths = pairedMonotonePaths(
    chartXs,
    chartYs[0],
    chartYs[1],
    OVERLAP_CENTER_DISTANCE,
  );

  function handlePointerMove(event: React.PointerEvent<SVGRectElement>) {
    const rect = event.currentTarget.getBoundingClientRect();
    const relativeX = ((event.clientX - rect.left) / rect.width) * WIDTH;
    const fraction = Math.min(Math.max((relativeX - MARGIN.left) / PLOT_WIDTH, 0), 1);
    const hoverTimestamp = startMs + fraction * timeRangeMs;
    const index = points.reduce((nearest, _point, candidate) => (
      Math.abs(intervalMidpointMs(candidate) - hoverTimestamp)
        < Math.abs(intervalMidpointMs(nearest) - hoverTimestamp)
        ? candidate
        : nearest
    ), 0);
    setHoverIndex(index);
  }

  const hovered = hoverIndex !== null ? points[hoverIndex] : null;
  const hoverFraction = hoverIndex !== null
    ? (xAt(hoverIndex) - MARGIN.left) / PLOT_WIDTH
    : 0;
  const tooltipTransform = hoverFraction < 0.15 ? "translateX(0)" : hoverFraction > 0.85 ? "translateX(-100%)" : "translateX(-50%)";

  return (
    <div className="chart-panel">
      <div className="chart-legend">
        {SERIES.map((series) => (
          <span key={series.key} className="chart-legend-item">
            <svg width="16" height="8" aria-hidden="true">
              <line x1="0" y1="4" x2="16" y2="4" stroke={series.color} strokeWidth={2} strokeLinecap="round" />
            </svg>
            {series.label}
          </span>
        ))}
        <span className="chart-legend-item">
          <svg width="16" height="8" aria-hidden="true">
            <line x1="0" y1="4" x2="16" y2="4" stroke={criticalColor} strokeWidth={1.5} strokeDasharray="4 4" />
          </svg>
          회수·공급 필요 시점
        </span>
      </div>
      <div className="chart-plot">
      <svg className="chart-svg" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label="12시간 대여·반납 예측 그래프">
        {hovered && (
          <rect
            data-hover-interval
            x={xAtTime(Date.parse(hovered.predicted_dttm) - FORECAST_INTERVAL_MS)}
            y={MARGIN.top}
            width={xAtTime(hovered.predicted_dttm)
              - xAtTime(Date.parse(hovered.predicted_dttm) - FORECAST_INTERVAL_MS)}
            height={PLOT_HEIGHT}
            fill="color-mix(in srgb, var(--brand-green) 9%, transparent)"
          />
        )}

        {yTicks.map((tick) => (
          <g key={tick}>
            <line
              x1={MARGIN.left}
              x2={WIDTH - MARGIN.right}
              y1={yAt(tick)}
              y2={yAt(tick)}
              stroke="var(--gridline)"
              strokeWidth={1}
            />
            <text x={MARGIN.left - 8} y={yAt(tick)} textAnchor="end" dominantBaseline="middle" fontSize={11} fill="var(--text-muted)">
              {tick}
            </text>
          </g>
        ))}

        {xTicks.map((timestamp, tickIndex) => (
          <text
            key={timestamp}
            x={xAtTime(timestamp)}
            y={HEIGHT - 5}
            textAnchor={tickIndex === 0 ? "start" : tickIndex === xTicks.length - 1 ? "end" : "middle"}
            fontSize={11}
            fill="var(--text-muted)"
          >
            {formatTime(new Date(timestamp).toISOString())}
          </text>
        ))}

        {SERIES.map((series, seriesIndex) => {
          // 중앙점 사이의 추세선은 유지하되 첫·마지막 시간 구간도 비어 보이지
          // 않도록 같은 구간값으로 기준 시각과 +12시간 경계까지 수평 연장한다.
          const path = seriesPaths[seriesIndex];
          return (
            <path
              key={series.key}
              data-series={series.key}
              d={path}
              fill="none"
              stroke={series.color}
              strokeWidth={SERIES_STROKE_WIDTH}
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          );
        })}

        {criticalPoint && (
          <g>
            <line
              x1={xAtTime(criticalPoint.predicted_dttm)}
              x2={xAtTime(criticalPoint.predicted_dttm)}
              y1={MARGIN.top}
              y2={HEIGHT - MARGIN.bottom}
              stroke={criticalColor}
              strokeWidth={1.5}
              strokeDasharray="4 4"
            />
            <text x={xAtTime(criticalPoint.predicted_dttm)} y={MARGIN.top - 4} textAnchor={criticalAnchor} fontSize={10} fontWeight={700} fill={criticalColor}>
              {formatTime(criticalPoint.predicted_dttm)} {ACTION_LABEL[criticalPoint.action_type]}
            </text>
          </g>
        )}

        {hoverIndex !== null && hovered && (
          <>
            <line
              x1={xAt(hoverIndex)}
              x2={xAt(hoverIndex)}
              y1={MARGIN.top}
              y2={HEIGHT - MARGIN.bottom}
              stroke="var(--baseline)"
              strokeWidth={1}
            />
            {SERIES.map((series) => (
              <circle
                key={series.key}
                cx={xAt(hoverIndex)}
                cy={visualYAt(series.key, hoverIndex)}
                r={4}
                fill={series.color}
                stroke="var(--surface-1)"
                strokeWidth={2}
              />
            ))}
          </>
        )}

        <rect
          x={MARGIN.left}
          y={MARGIN.top}
          width={PLOT_WIDTH}
          height={PLOT_HEIGHT}
          fill="transparent"
          onPointerMove={handlePointerMove}
          onPointerLeave={() => setHoverIndex(null)}
        />
      </svg>
      {hoverIndex !== null && hovered && (
        <div
          className="chart-tooltip"
          style={{
            left: `${(xAt(hoverIndex) / WIDTH) * 100}%`,
            transform: tooltipTransform,
          }}
        >
          <div className="time">
            {formatTime(new Date(
              Date.parse(hovered.predicted_dttm) - FORECAST_INTERVAL_MS,
            ).toISOString())}
            ~{formatTime(hovered.predicted_dttm)}
          </div>
          {SERIES.map((series) => (
            <div key={series.key} className="chart-tooltip-row">
              <svg width="10" height="8" aria-hidden="true">
                <line x1="0" y1="4" x2="10" y2="4" stroke={series.color} strokeWidth={2} strokeLinecap="round" />
              </svg>
              <span className="value">{hovered[series.key]}대</span>
              <span className="label">{series.label}</span>
            </div>
          ))}
        </div>
      )}
      </div>
    </div>
  );
}
