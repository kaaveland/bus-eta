import type {Partition, StatsDataResponse} from "./api.ts";
import {type TimeSlot, useTimeSlot} from "./UrlParams.ts";
import NavBar from "./components/NavBar.tsx";
import {Link} from "react-router-dom";

export interface AboutProps {
  partitions: Partition[]
  stats: StatsDataResponse
}

export default function About({partitions, stats}: AboutProps) {
  const slot: TimeSlot = useTimeSlot(partitions);
  const {
    arrivals_count,
    date_range,
    leg_count,
    aggregated_count,
    memory
  } = stats;
  const {start, end} = date_range;
  return (<>
    <div className="content">
      <NavBar slot={slot}/>
      <h2>About</h2>
      <p>This page is a tool for exploring delays and deviations in public transit in Norway over time and place.
      It was created by using data from <Link to="https://data.entur.no">data.entur.no</Link> and analyzing it with <Link to="https://duckdb.org">DuckDB</Link>.
      The initial release had a companion <Link to="https://arktekk.no/blogs/2025_entur_realtimedataset">blog post</Link> that explains some of the methods and motivations.</p>
      <p>The page focuses on analyzing legs, the travel between two subsequent stop places in a public transit journey,
      in particular how the travel time changes over time. Instead of focusing on how delayed the traffic is at a stop place, it
      seeks to discover which part of the journey that <em>caused</em> the delay.</p>
      <p>The data on this page is refreshed every night, around 4AM Europe/Oslo time. The currently loaded
      data set contains data from {start} to {end}. There are {aggregated_count} rows of data discoverable in the maps. These
      were created from {leg_count} legs from one stop to the next, created from {arrivals_count} arrival registrations at stop places.
        Weekend traffic is not included in any of the aggregates.
      All the data is being served from a DuckDB in-memory database that is currently using {Math.round(memory / 1e6)}MB RAM.</p>
      <p>You can read a <Link to="https://arktekk.no/blogs/2025_fire_and_forget_linux_p1">blog series</Link> about the infrastructure
      this runs on if you'd like. The <Link to="https://github.com/kaaveland/bus-eta">code</Link> is available under the MIT license. Feel
      free to inspect the network traffic if you'd like to access the underlying data and use it for something cool, it's open data
        available under the <Link to="https://data.norge.no/nlod/no/1.0">NLOD</Link> license from Entur.
      </p>
      <p>Most pages have URLs that can be shared to show what you're looking at to someone else. You'll notice the URL containing coordinates and a zoom level.</p>
    </div>
  </>);
}