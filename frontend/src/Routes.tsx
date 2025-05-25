import {BrowserRouter, Route, Routes} from "react-router-dom";
import type {Bootstrap} from "./api.ts";
import HotSpots from "./HotSpots.tsx";
import LegStats from "./LegStats.tsx";
import Comparison from "./Comparison.tsx";
import About from "./About.tsx";

export interface AppRoutesProps {
  bootstrap: Bootstrap
}

export const AppRoutes = ({bootstrap} : AppRoutesProps) => {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Comparison partitions={bootstrap.partitions} dataSources={bootstrap.dataSources}/>}/>
        <Route path="/:year/:month/:hour">
          <Route path="about" element={<About partitions={bootstrap.partitions} stats={bootstrap.stats} />}/>
          <Route path="hot-spots" element={<HotSpots partitions={bootstrap.partitions} />}/>
          <Route path="legs/" element={<LegStats partitions={bootstrap.partitions} dataSources={bootstrap.dataSources}/>}/>
          <Route path="legs/:dataSource" element={<LegStats partitions={bootstrap.partitions} dataSources={bootstrap.dataSources}/>}/>
          <Route path="legs/:dataSource/:lineRef" element={<LegStats partitions={bootstrap.partitions} dataSources={bootstrap.dataSources}/>}/>
          <Route path="comparison/" element={<Comparison partitions={bootstrap.partitions} dataSources={bootstrap.dataSources}/>}/>
          <Route path="comparison/:prevYear/:prevMonth" element={<Comparison partitions={bootstrap.partitions} dataSources={bootstrap.dataSources}/>}/>
          <Route path="comparison/:prevYear/:prevMonth/:dataSource" element={<Comparison partitions={bootstrap.partitions} dataSources={bootstrap.dataSources}/>}/>
        </Route>
      </Routes>
    </BrowserRouter>
  )
}