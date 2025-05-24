import type {Partition} from "./api.ts";
import {type TimeSlot, useTimeSlot} from "./UrlParams.ts";
import {useNavigate, useParams} from "react-router-dom";
import {useEffect} from "react";
import NavBar from "./components/NavBar.tsx";

export interface ComparisonProps {
  partitions: Partition[]
}

export default function Comparison({partitions}: ComparisonProps) {
  const slot: TimeSlot = useTimeSlot(partitions);
  const { prevMonth, prevYear } = useParams();
  const prevPartition = partitions.find(p => p.year === Number(prevYear) && p.month === Number(prevMonth)) ??
    partitions[partitions.length - 2];
  const navigate = useNavigate();

  useEffect(() => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/comparison/${prevPartition.year}/${prevPartition.month}`);
  }, [slot.partition.year, slot.partition.month, slot.hour, navigate, prevPartition.year, prevPartition.month]);

  return (
    <>
      <NavBar slot={slot}/>
      <h2>Comparison {slot.partition.year}/{slot.partition.month} with {prevPartition.year}/{prevPartition.month} at {slot.hour}:00</h2>
    </>
  )
}