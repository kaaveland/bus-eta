import {Link} from "react-router-dom";
import {type TimeSlot} from "../UrlParams";

interface NavBarProps {
  slot: TimeSlot
}

export default function NavBar({slot}: NavBarProps) {
  const basePath = `/${slot.partition.year}/${slot.partition.month}/${slot.hour}`;

  return (
    <nav>
      <Link className="navigation" to={`${basePath}/comparison`}>Comparison</Link>
      <Link className="navigation" to={`${basePath}/hot-spots`}>Hot Spots</Link>
      <Link className="navigation" to={`${basePath}/legs`}>Leg Stats</Link>
      <Link className="navigation" to={`${basePath}/about`}>About</Link>
    </nav>
  );
}