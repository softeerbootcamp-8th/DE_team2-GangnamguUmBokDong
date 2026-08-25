import { useState } from "react";
import type { ForecastPoint, StationSummary } from "../api";
import { monotonePath } from "../curve";
import { formatIsoTime } from "../format";

interface Props {
  station: StationSummary;
  baseDttm: string;
  points: ForecastPoint[];
}

const WIDTH = 600;
const HEIGHT = 220;
const MARGIN = { top: 16, right: 16, bottom: 24, left: 32 };
const PLOT_WIDTH = WIDTH - MARGIN.left - MARGIN.right;
const PLOT_HEIGHT = HEIGHT - MARGIN.top - MARGIN.bottom;
const X_TICK_INTERVAL_MS = 3 * 60 * 60 * 1000;

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

export function StockChart({ station, baseDttm, points }: Props) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  // 지금 재고를 시작점으로 두고 그 뒤에 12시간 예측치를 이어 붙인다.
  const series = [
    { time: baseDttm, bikes: station.parking_bike_tot_cnt },
    ...points.map((p) => ({ time: p.predicted_dttm, bikes: p.predicted_bikes })),
  ];

  // 비콘 기반 대여소라 반납이 안 막혀서 정원을 실제로 넘을 수 있다. 그 초과분이
  // 잘리지 않도록, 정원과 실제 최대값 중 큰 쪽을 기준으로 y축을 잡는다.
  const maxObserved = Math.max(...series.map((p) => p.bikes));
  const maxY = Math.max(station.hold_cnt, maxObserved) * 1.1;
  const startMs = Date.parse(baseDttm);
  const endMs = Date.parse(series[series.length - 1].time);
  const timeRangeMs = Math.max(1, endMs - startMs);
  const xAtTime = (time: string | number) => {
    const timestamp = typeof time === "number" ? time : Date.parse(time);
    return MARGIN.left + ((timestamp - startMs) / timeRangeMs) * PLOT_WIDTH;
  };
  const xAt = (i: number) => xAtTime(series[i].time);
  const yAt = (v: number) => MARGIN.top + (1 - v / maxY) * PLOT_HEIGHT;
  const yTicks = [0, Math.round(station.hold_cnt / 2), station.hold_cnt];
  const xTicks = tickTimes(startMs, endMs);
  function handlePointerMove(event: React.PointerEvent<SVGRectElement>) {
    const rect = event.currentTarget.getBoundingClientRect();
    const relativeX = ((event.clientX - rect.left) / rect.width) * WIDTH;
    const fraction = Math.min(Math.max((relativeX - MARGIN.left) / PLOT_WIDTH, 0), 1);
    const hoverTimestamp = startMs + fraction * timeRangeMs;
    const index = series.reduce((nearest, point, candidate) => (
      Math.abs(Date.parse(point.time) - hoverTimestamp)
        < Math.abs(Date.parse(series[nearest].time) - hoverTimestamp)
        ? candidate
        : nearest
    ), 0);
    setHoverIndex(index);
  }

  const hovered = hoverIndex !== null ? series[hoverIndex] : null;
  const hoverFraction = hoverIndex !== null
    ? (xAt(hoverIndex) - MARGIN.left) / PLOT_WIDTH
    : 0;
  const tooltipTransform = hoverFraction < 0.15 ? "translateX(0)" : hoverFraction > 0.85 ? "translateX(-100%)" : "translateX(-50%)";

  const path = monotonePath(series.map((_, i) => xAt(i)), series.map((p) => yAt(p.bikes)));

  return (
    <div className="chart-panel">
      <div className="chart-legend">
        <span className="chart-legend-item">
          <svg width="16" height="8" aria-hidden="true">
            <line x1="0" y1="4" x2="16" y2="4" stroke="var(--series-stock)" strokeWidth={2} strokeLinecap="round" />
          </svg>
          예측 재고
        </span>
        <span className="chart-legend-item">
          <svg width="16" height="8" aria-hidden="true">
            <line x1="0" y1="4" x2="16" y2="4" stroke="var(--baseline)" strokeWidth={2} strokeDasharray="3 2" />
          </svg>
          정원
        </span>
      </div>
      <div className="chart-plot">
        <svg className="chart-svg" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label="12시간 예측 재고 그래프">
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

          <line
            x1={MARGIN.left}
            x2={WIDTH - MARGIN.right}
            y1={yAt(station.hold_cnt)}
            y2={yAt(station.hold_cnt)}
            stroke="var(--baseline)"
            strokeWidth={1.5}
            strokeDasharray="4 3"
          />

          <path d={path} fill="none" stroke="var(--series-stock)" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" />
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
              <circle cx={xAt(hoverIndex)} cy={yAt(hovered.bikes)} r={4} fill="var(--series-stock)" stroke="var(--surface-1)" strokeWidth={2} />
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
            <div className="time">{formatTime(hovered.time)}</div>
            <div className="chart-tooltip-row">
              <svg width="10" height="8" aria-hidden="true">
                <line x1="0" y1="4" x2="10" y2="4" stroke="var(--series-stock)" strokeWidth={2} strokeLinecap="round" />
              </svg>
              <span className="value">{hovered.bikes}대</span>
              <span className="label">예측 재고</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
