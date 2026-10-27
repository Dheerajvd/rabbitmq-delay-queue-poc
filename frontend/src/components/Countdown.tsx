import { useEffect, useState } from "react";
import { countdownLabel } from "../format";

interface CountdownProps {
  status: string;
  expectedReadyAt: string;
}

export function Countdown({ status, expectedReadyAt }: CountdownProps) {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    if (status !== "WAITING") {
      return;
    }
    const timer = window.setInterval(() => setNow(Date.now()), 250);
    return () => window.clearInterval(timer);
  }, [status, expectedReadyAt]);

  if (status !== "WAITING") {
    return <span className="muted">—</span>;
  }

  const label = countdownLabel(expectedReadyAt, now);
  return <span className={label.elapsed ? "countdown elapsed" : "countdown"}>{label.text}</span>;
}
