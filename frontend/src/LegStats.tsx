import type {DataSources, Partition} from "./api.ts";
import {type TimeSlot, useTimeSlot} from "./UrlParams.ts";
import {useNavigate, useParams} from "react-router-dom";
import {useEffect} from "react";

export interface LegStatsProps {
  partitions: Partition[]
  dataSources: DataSources
}

export default function LegStats({partitions}: LegStatsProps) {
  const slot: TimeSlot = useTimeSlot(partitions);
  const { dataSource } = useParams();
  const ds = dataSource ?? "RUT";
  const navigate = useNavigate();

  useEffect(() => {
    void navigate(`/${slot.partition.year}/${slot.partition.month}/${slot.hour}/legs/${ds}`);
  }, [slot.partition.year, slot.partition.month, slot.hour, ds, navigate]);

  return (
    <h2>Leg stats {ds} {slot.partition.year}/{slot.partition.month} {slot.hour}:00</h2>
  )
}