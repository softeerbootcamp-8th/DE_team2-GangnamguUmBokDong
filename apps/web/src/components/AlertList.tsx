import type { Alert } from "../api";
import { formatUntilCritical, statusOf } from "../format";
import type { UrgencyTier } from "../format";

const TIER_COLOR: Record<UrgencyTier, string> = {
  critical: "var(--status-critical)",
  serious: "var(--status-serious)",
  warning: "var(--status-warning)",
  good: "var(--status-good)",
};

interface Props {
  alerts: Alert[];
  selectedStationId: string | null;
  onSelect: (stationId: string) => void;
}

export function AlertList({
  alerts,
  selectedStationId,
  onSelect,
}: Props) {
  const supplyAlerts = alerts.filter((alert) => alert.action_type === "supply_needed");
  const retrievalAlerts = alerts.filter((alert) => alert.action_type === "retrieval_needed");
  // /alerts는 조치가 필요 없는 normal 대여소도 함께 내려준다. 두 열에는 담지
  // 않지만, 목록이 전체가 아니라는 사실은 알려야 한다.
  const normalCount = alerts.length - supplyAlerts.length - retrievalAlerts.length;

  function renderColumn(items: Alert[], title: string, id: string) {
    return (
      <section className="alert-column" aria-labelledby={id}>
        <h3 id={id}>
          <span>{title}</span>
          <strong>{items.length}</strong>
        </h3>
        {items.length === 0 ? (
          <p className="empty-state">해당 대여소가 없습니다.</p>
        ) : (
          <ul className="alert-column-list">
            {items.map((alert) => {
              const status = statusOf(alert.urgency_score, alert.action_type);
              const isSelected = alert.sta_id === selectedStationId;
              return (
                <li key={alert.sta_id}>
                  <button
                    type="button"
                    className={`alert-item${isSelected ? " selected" : ""}`}
                    onClick={() => onSelect(alert.sta_id)}
                  >
                    <span
                      className="status-icon"
                      style={{ color: TIER_COLOR[status.tier] }}
                      aria-hidden="true"
                    >
                      {status.icon}
                    </span>
                    <span className="alert-item-body">
                      <span className="alert-item-name">{alert.sta_nm}</span>
                      <span className="alert-item-meta">
                        {status.label} · {formatUntilCritical(alert.minutes_until_critical)}
                      </span>
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </section>
    );
  }

  return (
    <div className="alert-list-wrap">
      <div className="alert-workspace">
        {renderColumn(supplyAlerts, "공급 필요", "supply-stations-heading")}
        {renderColumn(retrievalAlerts, "회수 필요", "retrieval-stations-heading")}
      </div>
      {normalCount > 0 && (
        <p className="column-note">
          조치가 필요 없는 대여소 {normalCount}곳은 지도에서만 확인할 수 있습니다.
        </p>
      )}
    </div>
  );
}
