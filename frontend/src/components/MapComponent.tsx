import React, {useEffect, useState} from 'react';
import Plot from 'react-plotly.js';
import {getRow, type Hour, type LegStats, type Partition} from "../api.ts";

export interface MapView {
  lat: number,
  lon: number,
  zoom: number
}

interface MapProps {
  name: string,
  partition: Partition,
  showHour: Hour,
  dataSource?: string
  lineRef?: string
  data: LegStats,
  view: MapView,
  onRelayout: (view: MapView) => void
}

const tooltipFrom = (partition: Partition, hour: number, i: number, stats: LegStats) => {
  const {
    name,
    to_stop,
    data_source,
    air_distance_meters,
    mean_hourly_duration,
    mean_monthly_duration,
    rush_intensity,
    hourly_quartile,
    monthly_duration,
    hourly_duration,
    hourly_delay,
    hourly_deviation,
    monthly_delay,
    hourly_count,
    monthly_count
  } = getRow(i, stats);

  return `${data_source} <b>${name}</b><br>
Air distance ${air_distance_meters}m<br>
Stats for ${partition.year}/${partition.month} between ${hour}:00-${hour+1}:00<br><br>
Rush intensity ${rush_intensity}<br>
Takes around ${hourly_duration}s-${hourly_quartile}s now vs ${monthly_duration}s normally<br>
Average ${mean_hourly_duration}s travel time now vs ${mean_monthly_duration}s rest of day<br>
Delayed ${hourly_delay}s at ${to_stop} now vs ${monthly_delay}s normally<br>
Collects ${hourly_deviation}s delay on this stretch<br>
${hourly_count} vehicle registrations this hour of ${monthly_count} this month
<extra></extra>`
};

export const MapComponent: React.FC<MapProps> = (props) => {
  const {name, partition, showHour, dataSource, lineRef, data, view, onRelayout} = props;
  const {year, month} = partition;
  const {hour} = showHour;

  const [mounted, setMounted] = useState(false);

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
          className="figure"
          data={[
            {
              // @ts-expect-error the dependency we use for type-checking is wrongly typed (-:
              type: 'scattermap',
              lat: data.lat,
              lon: data.lon,
              mode: 'markers',
              marker: {
                color: data.rush_intensity,
                size: 10,
                // @ts-expect-error this is actually available despite the typing
                colorscale: "Viridis",
                cmax: 5,
                cmin: 1,
                name: ""
              },
              hovertemplate: data.name.map((_, i) => tooltipFrom(partition, hour, i, data))
            }
          ]}
          layout={{
            margin: {
              l: 30,
              r: 30,
              t: 50,
              b: 50
            },
            autosize: true,
            height: 800,
            title: {
              text: `${name} for ${year}-${month.toString().padStart(2, '0')} between ${hour}:00 and ${hour + 1}:00${dataSource ? ` | Source: ${dataSource}` : ''}${lineRef ? ` | Line: ${lineRef}` : ''}`,
            },
            paper_bgcolor: '#f5f5f0',
            hoverlabel: {
              align: "left"
            },
            // @ts-expect-error the dependency we use for type-checking is wrongly typed (-:
            map: {
              center: {
                lat: view.lat,
                lon: view.lon,
              },
              zoom: view.zoom
            },
          }}
          config={{
            displayModeBar: false
          }}
          onRelayout={handleRelayout}
      ></Plot>
  );

};
