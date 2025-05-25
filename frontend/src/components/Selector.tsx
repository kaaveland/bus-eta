import React from "react";
import type {DataSources, Hour, Partition} from "../api.ts";

export interface Datasource {
  label: string,
  id: string
}

export const labelDatasources = (dataSources: DataSources): Datasource[] => {
  return Object.keys(dataSources).map((key) => ({
    id: key, label: dataSources[key] ?? key
  }));
}

export interface PartitionSelectorProps {
  partitions: Partition[]
  selected: Partition
  handleSelect: (partition: Partition) => void
}

export interface HourSelectorProps {
  selected: Hour,
  handleSelect: (hour: Hour) => void
}

export const PartitionSelector: React.FC<PartitionSelectorProps> = ({partitions, selected, handleSelect}) => {
  const selectedIndex = partitions.findIndex(p => p.year === selected.year && p.month === selected.month);
  return <label>
    Month
    <select value={selectedIndex} onChange={(e) => handleSelect(partitions[parseInt(e.target.value)])}>
      {partitions.map((p, i) => (
        <option key={i} value={i}>{`${p.year}-${p.month}`}</option>
      ))}
    </select>
  </label>
}

export const HourSelector: React.FC<HourSelectorProps> = ({selected, handleSelect}) => {
  return <label>
    Hour
    <select value={selected.hour} onChange={(e) => handleSelect({hour: parseInt(e.target.value)})}>
      {Array.from({length: 24}, (_, i) => (
        <option key={i} value={i}>
          {`${i}:00-${(i + 1) % 24}:00`}
        </option>
      ))}
    </select>
  </label>
};

export interface ViewSelectorProps {
  views: Datasource[]
  selected: Datasource
  handleSelect: (ds: Datasource) => void;
}

export const ViewSelector: React.FC<ViewSelectorProps> = (props: ViewSelectorProps) => {
  // This can't possibly be necessary?
  const selectedIndex = props.views.findIndex(v => v.id === props.selected.id);
  return <label>
    Datasource
    <select
      value={selectedIndex}
      onChange={(e) => props.handleSelect(props.views[parseInt(e.target.value)])}>
      {props.views.map((v, i) => (
        <option key={i} value={i}>{v.label}</option>
      ))}
    </select>
  </label>
};
