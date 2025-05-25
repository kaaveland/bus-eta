import {type DataSources, type Hour, leg_stats, type LegStats, type LineRef, linesFor, type Partition} from "./api.ts";
import {defaultMapView, relayoutMapHook, type TimeSlot, useTimeSlot} from "./UrlParams.ts";
import {useNavigate, useParams, useSearchParams} from "react-router-dom";
import {useEffect, useState} from "react";
import {MapComponent, type MapView} from "./components/MapComponent.tsx";
import NavBar from "./components/NavBar.tsx";
import {
  type Datasource,
  HourSelector,
  labelDatasources, LinerefSelector,
  PartitionSelector,
  ViewSelector
} from "./components/Selector.tsx";

export interface LegStatsProps {
  partitions: Partition[]
  dataSources: DataSources
}

export default function LegStats({partitions, dataSources}: LegStatsProps) {
  const slot: TimeSlot = useTimeSlot(partitions);
  const {dataSource, lineRef} = useParams();
  const lineRefSeg = lineRef ? `/${lineRef}` : "";
  const ds = Object.keys(dataSources).find(k => k === dataSource) ?? "RUT";
  const dsLabel = dataSources[ds] ?? ds;
  const [lineRefs, setLineRefs] = useState<LineRef[] | null>(null);

  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const [mapView, setMapView] = useState<MapView>(defaultMapView(searchParams));
  const rememberRelayout = relayoutMapHook(setSearchParams, setMapView, searchParams);

  const [mapData, setMapData] = useState<LegStats | null>(null);

  useEffect(() => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/legs/${ds}${lineRefSeg}`);
  }, [slot.partition.year, slot.partition.month, slot.hour, ds, navigate, lineRefSeg]);

  useEffect(() => {
    linesFor(ds).then(setLineRefs).catch(console.error);
  }, [ds, setLineRefs]);

  useEffect(() => {
    leg_stats(slot.partition.year, slot.partition.month, slot.hour, ds, lineRef ?? undefined)
      .then(setMapData)
      .catch(console.error);
  }, [slot.partition.year, slot.partition.month, slot.hour, ds, lineRef]);

  const setPartition = (partition: Partition) => {
    void navigate(`/${partition.year}/${partition.month}/${slot.hour}/legs/${ds}${lineRefSeg}`);
  };

  const setHour = (hour: Hour) => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${hour.hour}/legs/${ds}${lineRefSeg}`);
  };

  const setDataSource = (ds: Datasource) => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/legs/${ds.id}`);
  };

  const setLineRef = (lineRef: string | null) => {
    const lineRefSeg = lineRef ? `/${lineRef}` : "";
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/legs/${ds}${lineRefSeg}`);
  };


  return (
    <>
      <NavBar slot={slot}/>
      <h2>Leg stats {dsLabel} {slot.partition.year}/{slot.partition.month} {slot.hour}:00-{slot.hour + 1}:00</h2>
      <div className="controls">
        <PartitionSelector partitions={partitions} selected={slot.partition} handleSelect={setPartition}/>
        <HourSelector selected={{hour: slot.hour}} handleSelect={setHour}/>
        <ViewSelector selected={{id: ds, label: dataSources[ds] ?? ds}} views={labelDatasources(dataSources)}
                      handleSelect={setDataSource}/>
        <LinerefSelector selected={lineRef ?? null} handleSelect={setLineRef} lineRefs={lineRefs} defaultLabel="All"/>
      </div>

      {!mapData ? <p>Loading...</p> : <MapComponent name={`Leg stats ${lineRef ?? dsLabel}`} partition={slot.partition} showHour={{
        hour: slot.hour
      }} data={mapData} onRelayout={rememberRelayout} view={mapView}/>}
    </>
  )
}