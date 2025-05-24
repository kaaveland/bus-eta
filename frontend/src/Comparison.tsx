import {comparisonStats, type Hour, type LegComparison, type Partition} from "./api.ts";
import {defaultMapView, relayoutMapHook, type TimeSlot, useTimeSlot} from "./UrlParams.ts";
import {useNavigate, useParams, useSearchParams} from "react-router-dom";
import {useEffect, useState} from "react";
import NavBar from "./components/NavBar.tsx";
import {HourSelector, PartitionSelector} from "./components/Selector.tsx";
import type {MapView} from "./components/MapComponent.tsx";
import {ComparisonMap} from "./components/ComparisonMap.tsx";

export interface ComparisonProps {
  partitions: Partition[]
}

export default function Comparison({partitions}: ComparisonProps) {
  const slot: TimeSlot = useTimeSlot(partitions);
  const { prevMonth, prevYear } = useParams();
  const prevPartition = partitions.find(p => p.year === Number(prevYear) && p.month === Number(prevMonth)) ??
    partitions[partitions.length - 2];
  const navigate = useNavigate();

  useEffect(() => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/comparison/${prevPartition.year}/${prevPartition.month}`);
  }, [slot.partition.year, slot.partition.month, slot.hour, navigate, prevPartition.year, prevPartition.month]);

  const setPartition = (partition: Partition) => {
    void navigate(`/${partition.year}/${partition.month}/${slot.hour}/comparison/${prevPartition.year}/${prevPartition.month}`);
  };
  const setPrevPartition = (partition: Partition) => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/comparison/${partition.year}/${partition.month}`);
  };
  const setHour = (hour: Hour) => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${hour.hour}/comparison/${prevPartition.year}/${prevPartition.month}`);
  };
  const [comparisonData, setComparisonData] = useState<LegComparison | null>(null);
  const [searchParams, setSearchParams] = useSearchParams();
  const [mapView, setMapView] = useState<MapView>(defaultMapView(searchParams));
  const rememberRelayout = relayoutMapHook(setSearchParams, setMapView);

  useEffect(() => {
    comparisonStats({previous: prevPartition, current: slot.partition, hour: slot.hour})
      .then(setComparisonData)
      .catch(console.error);
  }, [prevPartition, slot.partition, slot.hour]);

  return (
    <>
      <NavBar slot={slot}/>
      <h2>Comparison {slot.partition.year}/{slot.partition.month} with {prevPartition.year}/{prevPartition.month} at {slot.hour}:00</h2>
      <div className="controls">
        <PartitionSelector partitions={partitions} selected={slot.partition} handleSelect={setPartition}/>
        <HourSelector selected={{hour: slot.hour}} handleSelect={setHour} />
        <PartitionSelector partitions={partitions} selected={prevPartition} handleSelect={setPrevPartition}/>
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