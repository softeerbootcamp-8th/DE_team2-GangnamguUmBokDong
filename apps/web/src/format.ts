export function formatClock(date: Date, opts: Intl.DateTimeFormatOptions = { hour: "2-digit", minute: "2-digit", second: "2-digit" }): string {
  return date.toLocaleTimeString("ko-KR", opts);
}

export function formatIsoTime(iso: string, opts?: Intl.DateTimeFormatOptions): string {
  return formatClock(new Date(iso), opts);
}

export const ACTION_LABEL: Record<"supply_needed" | "retrieval_needed" | "normal", string> = {
  supply_needed: "공급 필요",
  retrieval_needed: "회수 필요",
  normal: "정상",
};

export function formatUntilCritical(minutes: number): string {
  if (minutes <= 0) return "지금 위험";
  const hours = Math.round(minutes / 60);
  return `${hours}시간 후 위험`;
}

export type UrgencyTier = "critical" | "serious" | "warning" | "good";

export interface UrgencyStatus {
  tier: UrgencyTier;
  icon: string;
  label: string;
}

// "정상"은 재고 방향(action_type) 자체가 문제없을 때만 쓴다. 점수가 0이어도
// (예: 재고는 비었지만 곧 반납이 몰려 자연 해소되는 경우) 방향은 여전히
// supply_needed/retrieval_needed이므로 최소 "주의"로 분류한다.
export function statusOf(
  score: number,
  actionType: "supply_needed" | "retrieval_needed" | "normal",
): UrgencyStatus {
  if (score >= 50) return { tier: "critical", icon: "⚠", label: "긴급" };
  if (score >= 20) return { tier: "serious", icon: "⚠", label: "심각" };
  if (actionType !== "normal") return { tier: "warning", icon: "⚠", label: "주의" };
  return { tier: "good", icon: "✓", label: "정상" };
}
