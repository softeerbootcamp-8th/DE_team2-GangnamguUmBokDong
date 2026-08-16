import { useState } from "react";
import type { Alert } from "../api";
import { ACTION_LABEL, formatUntilCritical, statusOf } from "../format";
import type { UrgencyTier } from "../format";

const TIER_COLOR: Record<UrgencyTier, string> = {
  critical: "var(--status-critical)",
  serious: "var(--status-serious)",
  warning: "var(--status-warning)",
  good: "var(--status-good)",
};

type Tab = "all" | "supply_needed" | "retrieval_needed";

const TABS: { key: Tab; label: string }[] = [
  { key: "all", label: "전체" },
  { key: "supply_needed", label: "공급 필요" },
  { key: "retrieval_needed", label: "회수 필요" },
];

interface Props {
  alerts: Alert[];
  selectedStationId: number | null;
  onSelect: (stationId: number) => void;
}

export function AlertList({ alerts, selectedStationId, onSelect }: Props) {
  const [tab, setTab] = useState<Tab>("all");

  // 지역센터 필터는 지도와 공유해야 해서(같은 지역만 지도+리스트 동시에 보여야 함)
  // 이 컴포넌트 자체가 아니라 App.tsx가 들고 있다 — 여기 들어오는 alerts는 이미
  // 그 필터가 적용된 상태다.
  const filtered = tab === "all" ? alerts : alerts.filter((alert) => alert.action_type === tab);

  return (
    <div className="alert-list-wrap">
      <div className="alert-tabs" role="tablist">
        {TABS.map((t) => (
          <button
            key={t.key}
            type="button"
            role="tab"
            aria-selected={tab === t.key}
            className={`alert-tab${tab === t.key ? " active" : ""}`}
            onClick={() => setTab(t.key)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {filtered.length === 0 ? (
        <p className="empty-state">해당하는 대여소가 없습니다.</p>
      ) : (
        <ul className="alert-list">
          {filtered.map((alert) => {
            const status = statusOf(alert.urgency_score, alert.action_type);
            const isSelected = alert.sta_id === selectedStationId;
            return (
              <li key={alert.sta_id}>
                <button
                  type="button"
                  className={`alert-item${isSelected ? " selected" : ""}`}
                  onClick={() => onSelect(alert.sta_id)}
                >
                  <span className="status-icon" style={{ color: TIER_COLOR[status.tier] }} aria-hidden="true">
                    {status.icon}
                  </span>
                  <span className="alert-item-body">
                    <span className="alert-item-name">{alert.sta_nm}</span>
                    <span className="alert-item-meta">
                      {status.label} · {ACTION_LABEL[alert.action_type]}
                      {alert.action_type !== "normal" && ` · ${formatUntilCritical(alert.minutes_until_critical)}`}
                    </span>
                  </span>
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
