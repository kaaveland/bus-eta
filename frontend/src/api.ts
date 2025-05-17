// Find definitions/routes in api.py
const BASE_URL = "https://kollektivkart.kaveland.no/api/";

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

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
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
  air_distance_m: number[],
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
  return await fetchJson(`hot-spots/${year}/${month}/${hour}/${dataSource}?line_ref=${lineRef ?? ""}`)
}