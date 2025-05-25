// Find definitions/routes in api.py
const BASE_URL = "https://kollektivcache.b-cdn.net/api/";

export type StatsDataResponse = {
  aggregated_count: number,
  arrivals_count: number,
  date_range: {
    // TODO: Maybe there's some way to auto-json these into some date type?
    start: string,
    end: string,
  },
  leg_count: string,
  memory: number
};

type RecordFromTable<T extends { [key: string]: unknown[] }> = {
  [K in keyof T]: T[K] extends Array<infer U> ? U : never;
};

export function getRow<T extends { [key: string]: unknown[] }>(
  index: number,
  table: T
): RecordFromTable<T> {
  return Object.fromEntries(
    Object.entries(table).map(([key, column]) => [key, column[index]])
  ) as RecordFromTable<T>;
}

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: {
      'Accept': 'application/json'
    }
  });
  if (!res.ok) throw new Error(`Failed to fetch ${path}`);
  // eslint-disable-next-line @typescript-eslint/no-unsafe-return
  return res.json();
}

export async function fetchStats(): Promise<StatsDataResponse> {
  return await fetchJson("stats");
}

export type Partition = {
  year: number, month: number
};

export type Hour = {
  hour: number
};

export async function fetchPartitions(): Promise<Partition[]> {
    return await fetchJson("partitions");
}

export type DataSources = {
  [key: string]: string | null
};

export async function datasourceNames(): Promise<DataSources> {
    return await fetchJson("datasource-names");
}

export type LineRef = {
  label: string, line_ref: string
};

export async function linesFor(datasource: string): Promise<LineRef[]> {
  return await fetchJson(`lines/${datasource}`);
}

// These things rarely change
export type Bootstrap = {
  partitions: Partition[],
  stats: StatsDataResponse,
  dataSources: DataSources
};

export async function loadBootstrap(): Promise<Bootstrap> {
  const [stats, partitions, dataSources] = await Promise.all([
    fetchStats(),
    fetchPartitions(),
    datasourceNames(),
  ]);

  return {
    stats,
    partitions,
    dataSources,
  };
}

export type LegStats = {
  name: string[],
  from_stop: string[],
  to_stop: string[],
  data_source: string[],
  air_distance_meters: number[],
  lat: number[],
  lon: number[],
  rush_intensity: number[],
  hourly_quartile: number[],
  hourly_duration: number[],
  monthly_duration: number[],
  monthly_delay: number[],
  hourly_delay: number[],
  monthly_deviation: number[],
  hourly_deviation: number[],
  monthly_count: number[],
  hourly_count: number[]
};

export async function hotspots(year: number, month: number, hour: number): Promise<LegStats> {
  return await fetchJson(`hot-spots/${year}/${month}/${hour}`)
}

export async function leg_stats(year: number, month: number, hour: number, dataSource: string, lineRef?: string): Promise<LegStats> {
  const qparam = !lineRef ? "" : `?line_ref=${lineRef}`;
  return await fetchJson(`leg-stats/${year}/${month}/${hour}/${dataSource}${qparam}`);
}

export type LegComparison = {
  name: string[],
  from_stop: string[],
  to_stop: string[],
  data_source: string[],
  net_change_seconds: number[],
  net_change_proportion: number[],
  net_change_pct: number[],
  air_distance_meters: number[],
  from_lat: number[],
  from_lon: number[],
  to_lat: number[],
  to_lon: number[],
  lat: number[],
  lon: number[],
  cur_hourly_quartile: number[],
  prev_hourly_quartile: number[],
  cur_hourly_duration: number[],
  prev_hourly_duration: number[],
  cur_hourly_delay: number[],
  prev_hourly_delay: number[],
  cur_hourly_deviation: number[],
  prev_hourly_deviation: number[],
  cur_mean_hourly_duration: number[],
  prev_mean_hourly_duration: number[],
  cur_monthly_count: number[],
  prev_monthly_count: number[],
  cur_hourly_count: number[],
  prev_hourly_count: number[]
}

export type ComparisonParameters = {
  previous: Partition,
  current: Partition,
  hour: number,
  data_source?: string,
  line_ref?: string
};

export const comparisonStats = async ({previous, current, hour, data_source, line_ref}: ComparisonParameters) => {
  const qparams = [
    !line_ref ? "" : `line_ref=${line_ref}`,
    !data_source ? "" : `data_source=${data_source}`
  ].join("&");
  const qstring = !qparams ? "" : `?${qparams}`;
  return await fetchJson<LegComparison>(
    `comparison/${current.year}/${current.month}/${previous.year}/${previous.month}/${hour}${qstring}`
  );
}