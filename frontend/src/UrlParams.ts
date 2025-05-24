import type {Partition} from "./api.ts";
import {useParams} from "react-router-dom";

export interface TimeSlot {
  partition: Partition
  hour: number
}

export const useTimeSlot = (partitions: Partition[]): TimeSlot => {
  const {year, month, hour} = useParams();
  const defaultPartition = partitions[partitions.length - 1];
  const yearInt = parseInt(year ?? "0");
  const monthInt = parseInt(month ?? "0");
  const found = partitions.find(p => p.year === yearInt && p.month === monthInt);
  const partition: Partition = found ?? defaultPartition;
  const parsed = parseInt(hour ?? "15");
  const selectedHour: number = isNaN(parsed) || !parsed ? 15 : Math.min(23, Math.max(0, parsed));

  return {
    partition,
    hour: selectedHour
  };
};
