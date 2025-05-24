import {hotspots, type LegStats, type Partition} from "./api.ts";
import {useEffect, useState} from "react";
import {useNavigate} from "react-router-dom";
import {type TimeSlot, useTimeSlot} from "./UrlParams";
import {MapComponent, type MapView} from "./components/MapComponent.tsx";

export interface HotSpotsProps {
  partitions: Partition[]
}

export default function HotSpots({partitions}: HotSpotsProps) {
  const slot: TimeSlot = useTimeSlot(partitions);
  const navigate = useNavigate();

  useEffect(() => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/hot-spots`);
  }, [slot.partition.month, slot.partition.year, slot.hour, navigate]);

  const [mapView, setMapView] = useState<MapView>({
    lat: 60.91,
    lon: 8,
    zoom: 5
  });

  const [mapData, setMapData] = useState<LegStats | null>(null);

  useEffect(() => {
    hotspots(slot.partition.year, slot.partition.month, slot.hour)
      .then(setMapData)
      .catch(console.error);
  }, [slot.partition.year, slot.partition.month, slot.hour]);

  return (
    <>
      <h2>Hot spots {slot.partition.year}/{slot.partition.month} {slot.hour}:00</h2>
      {!mapData ? <p>Loading...</p> : <MapComponent name={"Hot spots"} partition={{
              year: slot.partition.year,
              month: slot.partition.month
          }} showHour={{
              hour: slot.hour
          }} data={mapData} onRelayout={setMapView} view={mapView} />}
    </>
  );
}