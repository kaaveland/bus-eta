import type {Partition} from "./api.ts";
import {useParams} from "react-router-dom";
import type {MapView} from "./components/MapComponent.tsx";

export interface TimeSlot {
  partition: Partition
  hour: number
}

export const parseNumericParam = (param: string | null, defaultValue: number) => {
    if (!param) return defaultValue;
    const parsed = parseFloat(param);
    return isFinite(parsed) ? parsed : defaultValue;
}

export const defaultMapView = (searchParams: URLSearchParams): MapView => {
  return {
    lat: parseNumericParam(searchParams.get('lat'), 60.91),
    lon: parseNumericParam(searchParams.get('lon'), 8),
    zoom: parseNumericParam(searchParams.get('zoom'), 5)
  }
}

export const relayoutMapHook = (
  setSearchParams: (params: URLSearchParams) => void, setMapView: (newView: MapView) => void, searchParams: URLSearchParams
) => {
  return (newView: MapView) => {
    const newParams = new URLSearchParams(searchParams);
    newParams.set('lat', newView.lat.toString());
    newParams.set('lon', newView.lon.toString());
    newParams.set('zoom', newView.zoom.toString());
    setSearchParams(newParams);
    return setMapView(newView);
  };
}

export const useTimeSlot = (partitions: Partition[]): TimeSlot => {
  const {year, month, hour} = useParams();
  const defaultPartition = partitions[partitions.length - 1];
  const yearInt = parseInt(year ?? "0");
  const monthInt = parseInt(month ?? "0");
  const found = partitions.find(p => p.year === yearInt && p.month === monthInt);
  const partition: Partition = found ?? defaultPartition;
  const parsed = parseInt(hour ?? "15");
  const selectedHour: number = isNaN(parsed) || !parsed ? 15 : Math.min(23, Math.max(0, parsed));

  return {
    partition,
    hour: selectedHour
  };
};
