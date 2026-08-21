import type { DispatchCenter } from "../api";

const ALL_REGIONS = "all";

interface Props {
  regions: DispatchCenter[];
  selectedRegion: string;
  onChange: (region: string) => void;
}

export function RegionTabs({ regions, selectedRegion, onChange }: Props) {
  return (
    <div className="filter-tab-row region-filter-row" role="group" aria-label="권역 필터">
      <button
        type="button"
        className={`alert-tab${selectedRegion === ALL_REGIONS ? " active" : ""}`}
        onClick={() => onChange(ALL_REGIONS)}
      >
        전체 권역
      </button>
      {regions.map((center) => (
        <button
          key={center.region}
          type="button"
          className={`alert-tab${selectedRegion === center.region ? " active" : ""}`}
          onClick={() => onChange(center.region)}
        >
          {center.region}
        </button>
      ))}
    </div>
  );
}
