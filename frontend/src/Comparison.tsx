import {comparisonStats, type DataSources, type Hour, type LegComparison, type Partition} from "./api.ts";
import {defaultMapView, relayoutMapHook, type TimeSlot, useTimeSlot} from "./UrlParams.ts";
import {useNavigate, useParams, useSearchParams} from "react-router-dom";
import {useEffect, useState} from "react";
import NavBar from "./components/NavBar.tsx";
import {DatasourceSelector, HourSelector, labelDatasources, PartitionSelector} from "./components/Selector.tsx";
import type {MapView} from "./components/MapComponent.tsx";
import {ComparisonMap} from "./components/ComparisonMap.tsx";

export interface ComparisonProps {
  partitions: Partition[]
  dataSources: DataSources
}

export default function Comparison({partitions, dataSources}: ComparisonProps) {
  const slot: TimeSlot = useTimeSlot(partitions);
  const { prevMonth, prevYear } = useParams();
  const prevPartition = partitions.find(p => p.year === Number(prevYear) && p.month === Number(prevMonth)) ??
    partitions[partitions.length - 2];
  const navigate = useNavigate();
  const { dataSource } = useParams();
  const dataSourceSeg = dataSource ? `/${dataSource}` : "";

  useEffect(() => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/comparison/${prevPartition.year}/${prevPartition.month}${dataSourceSeg}`);
  }, [slot.partition.year, slot.partition.month, slot.hour, navigate, prevPartition.year, prevPartition.month, dataSourceSeg]);

  const setPartition = (partition: Partition) => {
    void navigate(`/${partition.year}/${partition.month}/${slot.hour}/comparison/${prevPartition.year}/${prevPartition.month}${dataSourceSeg}`);
  };
  const setPrevPartition = (partition: Partition) => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/comparison/${partition.year}/${partition.month}${dataSourceSeg}`);
  };
  const setHour = (hour: Hour) => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${hour.hour}/comparison/${prevPartition.year}/${prevPartition.month}${dataSourceSeg}`);
  };
  const [comparisonData, setComparisonData] = useState<LegComparison | null>(null);
  const [searchParams, setSearchParams] = useSearchParams();
  const [mapView, setMapView] = useState<MapView>(defaultMapView(searchParams));
  const rememberRelayout = relayoutMapHook(setSearchParams, setMapView, searchParams);

  useEffect(() => {
    comparisonStats({previous: prevPartition, current: slot.partition, hour: slot.hour, data_source: dataSource})
      .then(setComparisonData)
      .catch(console.error);
  }, [prevPartition, slot.partition, slot.hour, dataSource]);

  const setDataSource = (ds: string | null) => {
    const seg = ds ? `/${ds}` : "";
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/comparison/${prevPartition.year}/${prevPartition.month}${seg}`);
  }

  return (
    <>
      <NavBar slot={slot}/>
      <h2>Comparison {slot.partition.year}/{slot.partition.month} with {prevPartition.year}/{prevPartition.month} at {slot.hour}:00</h2>
      <div className="controls">
        <PartitionSelector partitions={partitions} selected={slot.partition} handleSelect={setPartition}/>
        <HourSelector selected={{hour: slot.hour}} handleSelect={setHour} />
        <PartitionSelector partitions={partitions} selected={prevPartition} handleSelect={setPrevPartition}/>
        <DatasourceSelector dataSources={labelDatasources(dataSources)} handleSelect={setDataSource} selected={dataSource ?? null} defaultLabel="Top 2000"/>
      </div>
      { !comparisonData ? <p>Loading...</p> : <ComparisonMap
              name="Comparison"
              partition={slot.partition}
              prevPartition={prevPartition}
              showHour={{hour: slot.hour}}
              data={comparisonData}
              view={mapView}
              onRelayout={rememberRelayout} />}
    </>
  )
}