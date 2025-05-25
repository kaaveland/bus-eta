import {useEffect, useState} from "react";
import {getRow, type Hour, type LegComparison, type Partition} from "../api.ts";
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


const tooltipFrom = (from: Partition, to: Partition, hour: number, i: number, stats: LegComparison) => {
  const {
    name,
    data_source,
    air_distance_meters,
    to_stop,
    prev_hourly_delay,
    cur_hourly_delay,
    cur_hourly_count,
    prev_hourly_count,
    prev_mean_hourly_duration,
    cur_mean_hourly_duration,
    net_change_pct,
    net_change_seconds,
    prev_hourly_duration,
    cur_hourly_duration,
    prev_hourly_quartile,
    cur_hourly_quartile,
    prev_hourly_deviation,
    cur_hourly_deviation
  } = getRow(i, stats);

  return `${data_source} <b>${name}</b> between ${hour}:00-${hour+1}:00<br><br>
Air distance ${air_distance_meters}m<br>
Changes from ${from.year}/${from.month} to ${to.year}/${to.month}<br>
Typical delay at ${to_stop} ${prev_hourly_delay}s → ${cur_hourly_delay}s<br>
Average travel time ${prev_mean_hourly_duration}s → ${cur_mean_hourly_duration}s<br>
Changed ${net_change_seconds}s (${net_change_pct}%)<br>
Typical travel time ${prev_hourly_duration}s → ${cur_hourly_duration}s<br>
75% percentile travel time ${prev_hourly_quartile}s → ${cur_hourly_quartile}s<br>
Typical delay incurred here ${prev_hourly_deviation}s → ${cur_hourly_deviation}s<br>
Counted traffic ${prev_hourly_count} → ${cur_hourly_count}
<extra></extra>`
};

export const ComparisonMap: React.FC<ComparisonMapProps> = (props) => {
  const [mounted, setMounted] = useState(false);
  const { data, view, onRelayout, showHour, partition, prevPartition } = props;
  const {hour} = showHour;

  // Y U NO HAVE TUPLE COMPARISONS
  const isEarlier =
    prevPartition.year < partition.year ||
    (prevPartition.year === partition.year && prevPartition.month < partition.month);

  const [past, future] = isEarlier
    ? [prevPartition, partition]
    : [partition, prevPartition];

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
              name: ""
            },
            hovertemplate: data.name.map((_, i) => tooltipFrom(past, future, hour, i, data))
          }]}
          layout={{
            title: {
              text: `Comparing ${past.year}/${past.month} and ${future.year}/${future.month} between  ${hour}:00-${hour + 1}:00`
            },
            hoverlabel: {
              align: "left"
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