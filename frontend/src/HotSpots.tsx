import type {Partition} from "./api.ts";
import {useEffect} from "react";
import {useNavigate} from "react-router-dom";
import {type TimeSlot, useTimeSlot} from "./UrlParams";

export interface HotSpotsProps {
  partitions: Partition[]
}

export default function HotSpots({partitions}: HotSpotsProps) {
  const slot: TimeSlot = useTimeSlot(partitions);
  const navigate = useNavigate();

  useEffect(() => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/hot-spots`);
  }, [slot.partition.month, slot.partition.year, slot.hour, navigate]);

  return (
    <h2>Hot spots {slot.partition.year}/{slot.partition.month} {slot.hour}:00</h2>
  );
}