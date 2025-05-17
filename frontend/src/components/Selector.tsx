import React from "react";
import type {Hour, Partition} from "../api.ts";

export interface PartitionSelectorProps {
  partitions: Partition[]
  selected: Partition
  handleSelect: React.Dispatch<React.SetStateAction<Partition>>
}

export interface HourSelectorProps {
  selected: Hour,
  handleSelect: React.Dispatch<React.SetStateAction<Hour>>
}

export const PartitionSelector: React.FC<PartitionSelectorProps> = ({partitions, selected, handleSelect}) => {
  const selectedIndex = partitions.findIndex(p => p.year === selected.year && p.month === selected.month);
  return <label>
    Choose month
    <select value={selectedIndex} onChange={(e) => handleSelect(partitions[parseInt(e.target.value)])}>
      {partitions.map((p, i) => (
        <option key={i} value={i}>{`${p.year}-${p.month}`}</option>
      ))}
    </select>
  </label>
}

export const HourSelector: React.FC<HourSelectorProps> = ({selected, handleSelect}) => {
  return <label>
    Choose hour
    <select value={selected.hour} onChange={(e) => handleSelect({hour: parseInt(e.target.value)})}>
      {Array.from({length: 24}, (_, i) => (
        <option key={i} value={i}>
        {`${i}:00-${(i + 1) % 24}:00`}
        </option>
      ))}
    </select>
  </label>
};