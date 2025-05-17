import {type Bootstrap, hotspots, type Hour, type LegStats, type Partition} from "./api";
import {useEffect, useState} from "react";
import {MapComponent, type MapView} from "./components/MapComponent.tsx";
import {HourSelector, PartitionSelector} from "./components/Selector.tsx";

export default function App({bootstrap}: { bootstrap: Bootstrap }) {
  // Use this to navigate the map, for example, when fetching leg stats for a data source
  const [mapView, setMapView] = useState<MapView>({
    lat: 60.91,
    lon: 8,
    zoom: 5
  });
  const [globalPartition, setGlobalPartition] = useState<Partition>(
    bootstrap.partitions[bootstrap.partitions.length - 2]
  );
  const [globalHour, setGlobalHour] = useState<Hour>({
    hour: 15
  });
  const [mapData, setMapData] = useState<LegStats | null>(null);

  useEffect(() => {
    hotspots(globalPartition.year, globalPartition.month, globalHour.hour)
      .then(setMapData)
      .catch(console.error);
  }, [globalPartition, globalHour]);

  if (!mapData) {
    return <p>Loading...</p>
  } else {
    return <div>
      <PartitionSelector selected={globalPartition} partitions={bootstrap.partitions} handleSelect={setGlobalPartition}
      />
      <HourSelector selected={globalHour} handleSelect={setGlobalHour}/>
      <div style={{width: "100%"}}>
        <MapComponent partition={globalPartition} showHour={globalHour} data={mapData} view={mapView}
                      onRelayout={setMapView} />
      </div>
    </div>
  }
}