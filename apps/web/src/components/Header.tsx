import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { DispatchCenter, ServingHealthResponse, ServingHealthState } from "../api";
import { formatClock, formatIsoTime } from "../format";
import { RegionTabs } from "./RegionTabs";
import logo from "../../assets/ubd_logo.png";

const TOOLTIP_GAP_PX = 7;
const TOOLTIP_GUTTER_PX = 8;
const TOOLTIP_FALLBACK_WIDTH_PX = 250;
const HEALTH_TOOLTIP_FALLBACK_WIDTH_PX = 360;

interface Props {
  regions: DispatchCenter[];
  selectedRegion: string;
  stationsUpdatedAt: Date | null;
  servingHealth: ServingHealthResponse | null;
  servingHealthError: boolean;
  onRegionChange: (region: string) => void;
}

interface HeaderTimeProps {
  id: string;
  label: string;
  value: string;
  description: string;
  error?: boolean;
}

function HeaderTime({ id, label, value, description, error = false }: HeaderTimeProps) {
  const triggerRef = useRef<HTMLButtonElement>(null);
  const tooltipRef = useRef<HTMLSpanElement>(null);
  const [tooltipOpen, setTooltipOpen] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ left: 0, top: 0 });

  const updateTooltipPosition = useCallback(() => {
    const trigger = triggerRef.current;
    const tooltip = tooltipRef.current;
    if (!trigger || !tooltip) return;

    const triggerRect = trigger.getBoundingClientRect();
    const tooltipRect = tooltip.getBoundingClientRect();
    const availableWidth = Math.max(0, window.innerWidth - TOOLTIP_GUTTER_PX * 2);
    const tooltipWidth = Math.min(
      tooltipRect.width || TOOLTIP_FALLBACK_WIDTH_PX,
      availableWidth,
    );
    const maxLeft = Math.max(
      TOOLTIP_GUTTER_PX,
      window.innerWidth - tooltipWidth - TOOLTIP_GUTTER_PX,
    );
    const left = Math.min(
      Math.max(TOOLTIP_GUTTER_PX, triggerRect.right - tooltipWidth),
      maxLeft,
    );
    const tooltipHeight = tooltipRect.height;
    const preferredTop = triggerRect.bottom + TOOLTIP_GAP_PX;
    const top = preferredTop + tooltipHeight <= window.innerHeight - TOOLTIP_GUTTER_PX
      ? preferredTop
      : Math.max(TOOLTIP_GUTTER_PX, triggerRect.top - tooltipHeight - TOOLTIP_GAP_PX);

    setTooltipPosition({ left, top });
  }, []);

  useLayoutEffect(() => {
    if (!tooltipOpen) return;
    updateTooltipPosition();
    window.addEventListener("resize", updateTooltipPosition);
    window.addEventListener("scroll", updateTooltipPosition, true);
    return () => {
      window.removeEventListener("resize", updateTooltipPosition);
      window.removeEventListener("scroll", updateTooltipPosition, true);
    };
  }, [tooltipOpen, updateTooltipPosition]);

  const tooltip = (
    <span
      ref={tooltipRef}
      id={`${id}-tooltip`}
      className={`header-time-tooltip${tooltipOpen ? " is-visible" : ""}`}
      role="tooltip"
      style={{ left: tooltipPosition.left, top: tooltipPosition.top }}
    >
      {description}
    </span>
  );

  return (
    <span className={`header-time${error ? " status-error" : ""}`}>
      <span>{label} {value}</span>
      <span className="header-time-help">
        <button
          ref={triggerRef}
          type="button"
          className="header-time-info"
          aria-label={`${label} 설명`}
          aria-describedby={`${id}-tooltip`}
          onMouseEnter={() => setTooltipOpen(true)}
          onMouseLeave={() => setTooltipOpen(false)}
          onFocus={() => setTooltipOpen(true)}
          onBlur={() => setTooltipOpen(false)}
        >
          i
        </button>
      </span>
      {typeof document === "undefined" ? tooltip : createPortal(tooltip, document.body)}
    </span>
  );
}

const HEALTH_COMPONENTS = [
  ["stock", "대여소·재고"],
  ["demand", "수요예측"],
  ["urgency", "대여소 우선순위"],
  ["routes", "작업 추천"],
  ["weather", "날씨"],
  ["events", "행사"],
  ["regions", "권역 설정"],
] as const;

const HEALTH_STATE_LABEL: Record<ServingHealthState, string> = {
  ready: "정상",
  stale: "지연",
  expired: "사용 불가",
  missing: "미게시",
  misaligned: "기준 불일치",
};

function componentTime(value: string | null): string {
  if (!value) return "-";
  return formatIsoTime(value, { month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

function componentStateLabel(
  key: typeof HEALTH_COMPONENTS[number][0],
  state: ServingHealthState | undefined,
  reason: string | undefined,
): string {
  if (key === "weather" && reason === "weather_issue_stale") return "원본 지연";
  return state ? HEALTH_STATE_LABEL[state] : "확인 중";
}

function ServingHealthTime({
  health,
  error,
}: {
  health: ServingHealthResponse | null;
  error: boolean;
}) {
  const triggerRef = useRef<HTMLButtonElement>(null);
  const tooltipRef = useRef<HTMLSpanElement>(null);
  const [tooltipOpen, setTooltipOpen] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ left: 0, top: 0 });

  const updateTooltipPosition = useCallback(() => {
    const trigger = triggerRef.current;
    const tooltip = tooltipRef.current;
    if (!trigger || !tooltip) return;

    const triggerRect = trigger.getBoundingClientRect();
    const tooltipRect = tooltip.getBoundingClientRect();
    const availableWidth = Math.max(0, window.innerWidth - TOOLTIP_GUTTER_PX * 2);
    const tooltipWidth = Math.min(
      tooltipRect.width || HEALTH_TOOLTIP_FALLBACK_WIDTH_PX,
      availableWidth,
    );
    const maxLeft = Math.max(
      TOOLTIP_GUTTER_PX,
      window.innerWidth - tooltipWidth - TOOLTIP_GUTTER_PX,
    );
    const left = Math.min(
      Math.max(TOOLTIP_GUTTER_PX, triggerRect.right - tooltipWidth),
      maxLeft,
    );
    const tooltipHeight = tooltipRect.height;
    const preferredTop = triggerRect.bottom + TOOLTIP_GAP_PX;
    const top = preferredTop + tooltipHeight <= window.innerHeight - TOOLTIP_GUTTER_PX
      ? preferredTop
      : Math.max(TOOLTIP_GUTTER_PX, triggerRect.top - tooltipHeight - TOOLTIP_GAP_PX);

    setTooltipPosition({ left, top });
  }, []);

  useLayoutEffect(() => {
    if (!tooltipOpen) return;
    updateTooltipPosition();
    window.addEventListener("resize", updateTooltipPosition);
    window.addEventListener("scroll", updateTooltipPosition, true);
    return () => {
      window.removeEventListener("resize", updateTooltipPosition);
      window.removeEventListener("scroll", updateTooltipPosition, true);
    };
  }, [tooltipOpen, updateTooltipPosition]);

  const overall = error ? "unavailable" : health?.overall ?? "loading";
  const overallLabel = overall === "healthy"
    ? "정상"
    : overall === "degraded"
      ? "일부 지연"
      : overall === "unavailable"
        ? "연결 끊김"
        : "확인 중";
  const baseTime = health?.operational_base_dttm
    ? formatIsoTime(health.operational_base_dttm, { hour: "2-digit", minute: "2-digit" })
    : "-";

  const tooltip = (
    <span
      ref={tooltipRef}
      id="serving-health-tooltip"
      className={`serving-health-popover${tooltipOpen ? " is-visible" : ""}`}
      role="tooltip"
      aria-label="데이터 상태 상세"
      style={{ left: tooltipPosition.left, top: tooltipPosition.top }}
    >
      <span className="serving-health-popover-header">
        <strong>데이터 상태</strong>
      </span>
      <span className="serving-health-list">
        {HEALTH_COMPONENTS.map(([key, label]) => {
          const component = health?.components[key];
          const state = component?.state;
          return (
            <span key={key} className="serving-health-row">
              <span className={`serving-health-dot ${state ?? "loading"}`} aria-hidden="true" />
              <span className="serving-health-label">{label}</span>
              <span className={`serving-health-state ${state ?? "loading"}`}>
                {componentStateLabel(key, state, component?.reason)}
              </span>
              <time>
                <span>
                  {component?.source_dttm ? "게시 " : ""}
                  {componentTime(component?.data_dttm ?? null)}
                </span>
                {component?.source_dttm && (
                  <small>원본 {componentTime(component.source_dttm)}</small>
                )}
              </time>
            </span>
          );
        })}
      </span>
      <span className="serving-health-footer">
        {error
          ? "상태 조회 실패 · 마지막 정상 화면을 유지합니다."
          : health
            ? `마지막 확인 ${componentTime(health.checked_at)}`
            : "데이터 상태를 확인하고 있습니다."}
      </span>
    </span>
  );

  return (
    <span className="header-time serving-health-time">
      <span>기준 시각 {baseTime}</span>
      <span className="header-time-help serving-health-help">
        <button
          ref={triggerRef}
          type="button"
          className="header-time-info"
          aria-label="기준 시각 및 데이터 상태 설명"
          aria-describedby="serving-health-tooltip"
          onMouseEnter={() => setTooltipOpen(true)}
          onMouseLeave={() => setTooltipOpen(false)}
          onFocus={() => setTooltipOpen(true)}
          onBlur={() => setTooltipOpen(false)}
        >
          i
        </button>
      </span>
      {typeof document === "undefined" ? tooltip : createPortal(tooltip, document.body)}
      <span className={`serving-health-badge ${overall}`}>{overallLabel}</span>
    </span>
  );
}

export function Header({
  regions,
  selectedRegion,
  stationsUpdatedAt,
  servingHealth,
  servingHealthError,
  onRegionChange,
}: Props) {
  const [now, setNow] = useState(new Date());

  useEffect(() => {
    const clock = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(clock);
  }, []);

  return (
    <header className="app-header">
      <span className="app-header-title">
        <img src={logo} alt="" className="app-header-logo" />
        <h1>서울특별시 따릉이 재배치 대시보드</h1>
        <RegionTabs
          regions={regions}
          selectedRegion={selectedRegion}
          onChange={onRegionChange}
        />
      </span>
      <div className="app-header-times">
        <HeaderTime
          id="current-time"
          label="현재 시각"
          value={formatClock(now)}
          description="이 브라우저 기기의 현재 시각입니다."
        />
        <HeaderTime
          id="station-query-time"
          label="조회 시각"
          value={stationsUpdatedAt ? formatClock(stationsUpdatedAt) : "-"}
          description="현재 화면의 대여소 정보를 API에서 성공적으로 조회한 시각입니다. 조회에 실패하면 -로 표시됩니다."
        />
        <ServingHealthTime health={servingHealth} error={servingHealthError} />
      </div>
    </header>
  );
}
