import {type DataSources, type Hour, leg_stats, type LegStats, type Partition} from "./api.ts";
import {defaultMapView, relayoutMapHook, type TimeSlot, useTimeSlot} from "./UrlParams.ts";
import {useNavigate, useParams, useSearchParams} from "react-router-dom";
import {useEffect, useState} from "react";
import {MapComponent, type MapView} from "./components/MapComponent.tsx";
import NavBar from "./components/NavBar.tsx";
import {
  type Datasource,
  HourSelector,
  labelDatasources,
  PartitionSelector,
  ViewSelector
} from "./components/Selector.tsx";

export interface LegStatsProps {
  partitions: Partition[]
  dataSources: DataSources
}

export default function LegStats({partitions, dataSources}: LegStatsProps) {
  const slot: TimeSlot = useTimeSlot(partitions);
  const {dataSource} = useParams();
  const ds = Object.keys(dataSources).find(k => k === dataSource) ?? "RUT";
  const dsLabel = dataSources[ds] ?? ds;

  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const [mapView, setMapView] = useState<MapView>(defaultMapView(searchParams));
  const rememberRelayout = relayoutMapHook(setSearchParams, setMapView);

  const [mapData, setMapData] = useState<LegStats | null>(null);

  useEffect(() => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/legs/${ds}`);
  }, [slot.partition.year, slot.partition.month, slot.hour, ds, navigate]);

  useEffect(() => {
    leg_stats(slot.partition.year, slot.partition.month, slot.hour, ds)
      .then(setMapData)
      .catch(console.error);
  }, [slot.partition.year, slot.partition.month, slot.hour, ds]);

  const setPartition = (partition: Partition) => {
    void navigate(`/${partition.year}/${partition.month}/${slot.hour}/legs/${ds}`);
  };

  const setHour = (hour: Hour) => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${hour.hour}/legs/${ds}`);
  };

  const setDataSource = (ds: Datasource) => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/legs/${ds.id}`);
  };

  return (
    <>
      <NavBar slot={slot}/>
      <h2>Leg stats {dsLabel} {slot.partition.year}/{slot.partition.month} {slot.hour}:00-{slot.hour + 1}:00</h2>
      <div className="controls">
        <PartitionSelector partitions={partitions} selected={slot.partition} handleSelect={setPartition}/>
        <HourSelector selected={{hour: slot.hour}} handleSelect={setHour} />
        <ViewSelector selected={{id: ds, label: dataSources[ds] ?? ds}} views={labelDatasources(dataSources)} handleSelect={setDataSource}/>
      </div>

      {!mapData ? <p>Loading...</p> : <MapComponent name={`Leg stats ${dsLabel}`} partition={slot.partition} showHour={{
        hour: slot.hour
      }} data={mapData} onRelayout={rememberRelayout} view={mapView}/>}
    </>
  )
}