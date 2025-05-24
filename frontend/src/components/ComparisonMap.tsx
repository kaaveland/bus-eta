import {useEffect, useState} from "react";
import type {Hour, LegComparison, Partition} from "../api.ts";
import type {MapView} from "./MapComponent.tsx";
import Plot from "react-plotly.js";

export interface ComparisonMapProps {
  name: string,
  partition: Partition,
  prevPartition: Partition,
  showHour: Hour,
  data: LegComparison,
  view: MapView,
  onRelayout: (view: MapView) => void
}

export const ComparisonMap: React.FC<ComparisonMapProps> = (props) => {
  const [mounted, setMounted] = useState(false);
  const { data, view, onRelayout, showHour, partition, prevPartition } = props;
  const {hour} = showHour;

  useEffect(() => {
    setMounted(true);
  }, []);

  const handleRelayout = (event: unknown) => {
    const updated = { ...view };

    if (event && typeof event === 'object') {
      const typedEvent = event as Record<string, unknown>;

      if (typedEvent['map.center']) {
        const center = typedEvent['map.center'] as { lat: number, lon: number };
        updated.lat = center.lat;
        updated.lon = center.lon;
      }
      if (typedEvent['map.zoom']) {
        updated.zoom = typedEvent['map.zoom'] as number;
      }
    }

    onRelayout(updated);
  };

  return (mounted &&
      <Plot
          useResizeHandler={true}
          data={[{
            // @ts-expect-error the dependency we use for type-checking is wrongly typed (-:
            type: "scattermap",
            lat: data.lat,
            lon: data.lon,
            mode: "markers",
            marker: {
              color: data.net_change_proportion,
              size: 10,
              // @ts-expect-error this is actually available despite the typing
              colorscale: "Viridis",
              cmax: 50,
              cmin: -50,
            }
          }]}
          layout={{
            title: {
              text: `Comparing ${partition.year}/${partition.month} and ${prevPartition.year}/${prevPartition.month} between  ${hour}:00-${hour + 1}:00`
            },
            autosize: true,
            height: 800,
            // @ts-expect-error the dependency we use for type-checking is wrongly typed (-:
            map: {
              center: {
                lat: view.lat,
                lon: view.lon
              },
              zoom: view.zoom
            }
          }}
          config={{
            displayModeBar: false
          }}
          onRelayout={handleRelayout}
      />
  )
};