import {type DataSources, leg_stats, type LegStats, type Partition} from "./api.ts";
import {type TimeSlot, useTimeSlot} from "./UrlParams.ts";
import {useNavigate, useParams, useSearchParams} from "react-router-dom";
import {useEffect, useState} from "react";
import {MapComponent, type MapView} from "./components/MapComponent.tsx";

export interface LegStatsProps {
  partitions: Partition[]
  dataSources: DataSources
}

export default function LegStats({partitions, dataSources}: LegStatsProps) {
  const slot: TimeSlot = useTimeSlot(partitions);
  const {dataSource} = useParams();
  const ds = Object.keys(dataSources).find(k => k === dataSource) ?? "RUT";
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  const parseNumericParam = (param: string | null, defaultValue: number): number => {
    if (!param) return defaultValue;
    const parsed = parseFloat(param);
    return isFinite(parsed) ? parsed : defaultValue;
  };

  const [mapView, setMapView] = useState<MapView>({
    lat: parseNumericParam(searchParams.get('lat'), 60.91),
    lon: parseNumericParam(searchParams.get('lon'), 8),
    zoom: parseNumericParam(searchParams.get('zoom'), 5)
  });

  const rememberRelayout = (newView: MapView) => {
    const params = new URLSearchParams();
    params.set('lat', newView.lat.toString());
    params.set('lon', newView.lon.toString());
    params.set('zoom', newView.zoom.toString());
    setSearchParams(params);
    return setMapView(newView);
  };

  const [mapData, setMapData] = useState<LegStats | null>(null);

  useEffect(() => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/legs/${ds}`);
  }, [slot.partition.year, slot.partition.month, slot.hour, ds, navigate]);

  useEffect(() => {
    leg_stats(slot.partition.year, slot.partition.month, slot.hour, ds)
      .then(setMapData)
      .catch(console.error);
  }, [slot.partition.year, slot.partition.month, slot.hour, ds]);

  return (
    <>
      <h2>Leg stats {ds} {slot.partition.year}/{slot.partition.month} {slot.hour}:00</h2>
      {!mapData ? <p>Loading...</p> : <MapComponent name={`Leg stats ${ds}`} partition={{
        year: slot.partition.year,
        month: slot.partition.month
      }} showHour={{
        hour: slot.hour
      }} data={mapData} onRelayout={rememberRelayout} view={mapView}/>}
    </>
  )
}