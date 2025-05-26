import {hotspots, type Hour, type LegStats, type Partition} from "./api.ts";
import {useEffect, useState} from "react";
import {useNavigate, useSearchParams} from "react-router-dom";
import {defaultMapView, relayoutMapHook, type TimeSlot, useTimeSlot} from "./UrlParams";
import {MapComponent, type MapView} from "./components/MapComponent.tsx";
import NavBar from "./components/NavBar.tsx";
import {HourSelector, PartitionSelector} from "./components/Selector.tsx";

export interface HotSpotsProps {
  partitions: Partition[]
}

export default function HotSpots({partitions}: HotSpotsProps) {
  const slot: TimeSlot = useTimeSlot(partitions);
  const navigate = useNavigate();

  useEffect(() => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/hot-spots`);
  }, [slot.partition.month, slot.partition.year, slot.hour, navigate]);

  const [searchParams, setSearchParams] = useSearchParams();
  const [mapView, setMapView] = useState<MapView>(defaultMapView(searchParams));
  const rememberRelayout = relayoutMapHook(setSearchParams, setMapView, searchParams);

  const [mapData, setMapData] = useState<LegStats | null>(null);

  useEffect(() => {
    hotspots(slot.partition.year, slot.partition.month, slot.hour)
      .then(setMapData)
      .catch(console.error);
  }, [slot.partition.year, slot.partition.month, slot.hour]);

  const setPartition = (partition: Partition) => {
    void navigate(`/${partition.year}/${partition.month}/${slot.hour}/hot-spots`);
  };
  const setHour = (hour: Hour) => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${hour.hour}/hot-spots`);
  };

  return (
    <>
      <div className="content">
        <NavBar slot={slot}/>
        <h2>Hot spots</h2>
        <div className="controls">
          <PartitionSelector partitions={partitions} selected={slot.partition} handleSelect={setPartition}/>
          <HourSelector selected={{hour: slot.hour}} handleSelect={setHour} />
        </div>
        {!mapData ? <p>Loading...</p> : <MapComponent name={"Hot spots"} partition={{
                year: slot.partition.year,
                month: slot.partition.month
            }} showHour={{
                hour: slot.hour
            }} data={mapData} onRelayout={rememberRelayout} view={mapView} />}
      </div>
    </>
  );
}