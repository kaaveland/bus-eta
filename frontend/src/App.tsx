import {type Bootstrap, type DataSources, hotspots, type Hour, leg_stats, type LegStats, type Partition} from "./api";
import {useEffect, useState} from "react";
import {MapComponent, type MapView} from "./components/MapComponent.tsx";
import {HourSelector, PartitionSelector, ViewSelector} from "./components/Selector.tsx";


export interface LineRef {
  dataSource: Datasource
  label: string
  lineRef: string
}

export type Datasource = {
  label?: string
  id: string
}

export type View = Datasource | "Hot spots"

export const titleFor = (view: View) => view === "Hot spots" ? view : (view.label ?? view.id);
export const label = (dataSources: DataSources) => (
  Object.keys(dataSources).map((key) => ({
    id: key,
    label: dataSources[key] ?? key
  }))
)

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
  const [view, setView] = useState<View>("Hot spots");

  useEffect(() => {
    if (view === "Hot spots") {
      hotspots(globalPartition.year, globalPartition.month, globalHour.hour)
        .then(setMapData)
        .catch(console.error)
    } else {
      leg_stats(globalPartition.year, globalPartition.month, globalHour.hour, view.id)
        .then(setMapData)
        .catch(console.error);
    }
  }, [globalPartition, globalHour, view]);

  useEffect(() => {
    if (view !== "Hot spots" && mapData) {
      const meanLat = mapData.lat.reduce((a, b) => a + b, 0) / mapData.lat.length;
      const meanLon = mapData.lon.reduce((a, b) => a + b, 0) / mapData.lon.length;
      setMapView((m: MapView) => ({...m, lat: meanLat, lon: meanLon}));
    }
  }, [view, mapData]);

  if (!mapData) {
    return <p>Loading...</p>
  } else {
    return <div>
      <div className="header">
        <h1>Kollektivkart</h1>
        <a href="https://kollektivkart.arktekk.no">Legacy</a>
      </div>
      <div className="controls">
        <PartitionSelector selected={globalPartition} partitions={bootstrap.partitions}
                           handleSelect={setGlobalPartition} />
        <HourSelector selected={globalHour} handleSelect={setGlobalHour}/>
        <ViewSelector selected={view} views={["Hot spots", ...label(bootstrap.dataSources)]} handleSelect={setView}/>
      </div>
      <div style={{width: "100%"}}>
        <MapComponent name={titleFor(view)} partition={globalPartition} showHour={globalHour} data={mapData} view={mapView}
                      onRelayout={setMapView}/>
      </div>
    </div>
  }
}