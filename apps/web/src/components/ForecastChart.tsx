import { useState } from "react";
import type { ForecastPoint } from "../api";
import { monotonePath } from "../curve";
import { ACTION_LABEL, formatIsoTime } from "../format";

interface Props {
  points: ForecastPoint[];
}

const WIDTH = 600;
const HEIGHT = 220;
const MARGIN = { top: 16, right: 16, bottom: 24, left: 32 };
const PLOT_WIDTH = WIDTH - MARGIN.left - MARGIN.right;
const PLOT_HEIGHT = HEIGHT - MARGIN.top - MARGIN.bottom;

// 대여가 늘면 재고가 부족해지고(공급필요, 빨강), 반납이 늘면 재고가 넘친다
// (회수필요, 파랑) — 지도 마커의 방향별 색과 같은 의미로 맞춘다.
const SERIES = [
  { key: "predicted_rent_cnt" as const, label: "대여 예측", color: "var(--diverging-red)" },
  { key: "predicted_return_cnt" as const, label: "반납 예측", color: "var(--diverging-blue)" },
];

function formatTime(iso: string): string {
  return formatIsoTime(iso, { hour: "2-digit", minute: "2-digit" });
}

export function ForecastChart({ points }: Props) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  if (points.length === 0) {
    return <p className="empty-state">예측 데이터가 없습니다.</p>;
  }

  const maxY = Math.max(1, ...points.flatMap((p) => [p.predicted_rent_cnt, p.predicted_return_cnt])) * 1.2;
  const xAt = (i: number) => MARGIN.left + (i / (points.length - 1)) * PLOT_WIDTH;
  const yAt = (v: number) => MARGIN.top + (1 - v / maxY) * PLOT_HEIGHT;
  const yTicks = [0, Math.round(maxY / 2), Math.round(maxY)];
  const last = points[points.length - 1];
  const criticalIndex = points.findIndex((p) => p.action_type !== "normal");
  const criticalPoint = criticalIndex >= 0 ? points[criticalIndex] : null;
  // 이 선의 역할은 "언제 문제가 생기는지" 경고이지, 방향(공급/회수) 표시가 아니다
  // — 방향은 옆에 붙는 라벨 텍스트가 이미 말해준다. 데이터 선(빨강/파랑)과 같은
  // 색 계열을 쓰면 구분이 안 된다는 의견이 있어서, 경고를 뜻하는 색으로 뺐다.
  const criticalColor = "var(--status-warning)";
  const criticalAnchor = criticalIndex <= 1 ? "start" : criticalIndex >= points.length - 2 ? "end" : "middle";

  function handlePointerMove(event: React.PointerEvent<SVGRectElement>) {
    const rect = event.currentTarget.getBoundingClientRect();
    const relativeX = ((event.clientX - rect.left) / rect.width) * WIDTH;
    const index = Math.round(((relativeX - MARGIN.left) / PLOT_WIDTH) * (points.length - 1));
    setHoverIndex(Math.min(Math.max(index, 0), points.length - 1));
  }

  const hovered = hoverIndex !== null ? points[hoverIndex] : null;
  const hoverFraction = hoverIndex !== null ? hoverIndex / (points.length - 1) : 0;
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
      </div>
      <div className="chart-plot">
      <svg className="chart-svg" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label="12시간 대여·반납 예측 그래프">
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

        {SERIES.map((series) => {
          const xs = points.map((_, i) => xAt(i));
          const ys = points.map((p) => yAt(p[series.key]));
          const path = monotonePath(xs, ys);
          return (
            <g key={series.key}>
              <path d={path} fill="none" stroke={series.color} strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" />
              <circle cx={xAt(points.length - 1)} cy={yAt(last[series.key])} r={4} fill={series.color} stroke="var(--surface-1)" strokeWidth={2} />
              <text
                x={xAt(points.length - 1) - 6}
                y={yAt(last[series.key]) - 10}
                textAnchor="end"
                fontSize={11}
                fontWeight={700}
                fill="var(--text-primary)"
              >
                {last[series.key]}
              </text>
            </g>
          );
        })}

        {criticalPoint && (
          <g>
            <line
              x1={xAt(criticalIndex)}
              x2={xAt(criticalIndex)}
              y1={MARGIN.top}
              y2={HEIGHT - MARGIN.bottom}
              stroke={criticalColor}
              strokeWidth={1.5}
            />
            <text x={xAt(criticalIndex)} y={MARGIN.top - 4} textAnchor={criticalAnchor} fontSize={10} fontWeight={700} fill={criticalColor}>
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
                cy={yAt(hovered[series.key])}
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
          <div className="time">{formatTime(hovered.predicted_dttm)}</div>
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
