import React, {useEffect, useState} from 'react';
import Plot from 'react-plotly.js';
import type {Hour, LegStats, Partition} from "../api.ts";

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


export const MapComponent: React.FC<MapProps> = (props) => {
  const {name, partition, showHour, dataSource, lineRef, data, view, onRelayout} = props;
  const {year, month} = partition;
  const {hour} = showHour;

  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  const makeTooltip = (hour: number) => {
    const this_hour = `between ${hour}:00 and ${hour + 1}:00`;
    const col = (idx: number, fmt = "") => `%{customdata[${idx}]${fmt}}`;
    // I am sorry
    return `
  <b>${col(0)}</b> ${this_hour}<br>
  Air distance ${col(1)}m<br><br>
  Rush intensity ${col(2, ":.1f")}, 25% of transports take longer than ${col(3)}s ${this_hour}<br>
  ${col(10)} vehicles recorded for this month and ${col(11)} ${this_hour}<br>
  Monthly median travel time ${col(5)}s, ${col(4)}s ${this_hour}<br>
  Monthly median delay is ${col(7)}s, ${col(6)}s ${this_hour}<br>
  Monthly median deviation is ${col(9)}s, ${col(8)}s ${this_hour}
  <extra></extra>
  `;
  };

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
              customdata: data.name.map((_, i) => [
                data.name[i],
                data.air_distance_meters[i],
                data.rush_intensity[i],
                data.hourly_quartile[i],
                data.hourly_duration[i],
                data.monthly_duration[i],
                data.hourly_delay[i],
                data.monthly_delay[i],
                data.hourly_deviation[i],
                data.monthly_deviation[i],
                data.hourly_count[i],
                data.monthly_count[i],
              ]),
              hovertemplate: makeTooltip(hour)
            }
          ]}
          layout={{
            autosize: true,
            height: 800,
            title: {
              text: `${name} for ${year}-${month.toString().padStart(2, '0')} between ${hour}:00 and ${hour + 1}:00${dataSource ? ` | Source: ${dataSource}` : ''}${lineRef ? ` | Line: ${lineRef}` : ''}`,
            },
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
