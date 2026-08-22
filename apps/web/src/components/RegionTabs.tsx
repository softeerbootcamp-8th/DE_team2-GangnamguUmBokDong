import type { DispatchCenter } from "../api";

const ALL_REGIONS = "all";

interface Props {
  regions: DispatchCenter[];
  selectedRegion: string;
  onChange: (region: string) => void;
}

export function RegionTabs({ regions, selectedRegion, onChange }: Props) {
  return (
    <select
      className="region-select"
      aria-label="권역 선택"
      value={selectedRegion}
      onChange={(event) => onChange(event.target.value)}
    >
      <option value={ALL_REGIONS}>전체 권역</option>
      {regions.map((center) => (
        <option key={center.region} value={center.region}>
          {center.region}
        </option>
      ))}
    </select>
  );
}
