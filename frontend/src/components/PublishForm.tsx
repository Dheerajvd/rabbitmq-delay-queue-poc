import { FormEvent, useState } from "react";

interface PublishFormProps {
  disabled: boolean;
  error: string | null;
  onSubmit: (message: string, processingDurationSeconds: number, delayMinutes: number) => Promise<void>;
}

const delayOptions = [
  { minutes: 1, label: "1 min" },
  { minutes: 2, label: "2 min" },
  { minutes: 5, label: "5 min" },
];

export function PublishForm({ disabled, error, onSubmit }: PublishFormProps) {
  const [message, setMessage] = useState("Sample WhatsApp message");
  const [delayMinutes, setDelayMinutes] = useState(5);
  const [processing, setProcessing] = useState(2);
  const [localError, setLocalError] = useState<string | null>(null);

  async function handleSubmit(event: FormEvent) {
    event.preventDefault();
    const trimmed = message.trim();
    if (!trimmed) {
      setLocalError("Message is required.");
      return;
    }
    if (![1, 2, 5].includes(delayMinutes)) {
      setLocalError("Delay must be 1, 2, or 5 minutes.");
      return;
    }
    if (!Number.isInteger(processing) || processing < 0 || processing > 30) {
      setLocalError("Processing duration must be a whole number from 0 to 30 seconds.");
      return;
    }
    setLocalError(null);
    await onSubmit(trimmed, processing, delayMinutes);
  }

  const shown = localError || error;

  return (
    <form className="card" onSubmit={handleSubmit}>
      <h2>Publish job</h2>
      <label>
        Delay
        <select value={delayMinutes} onChange={(event) => setDelayMinutes(Number(event.target.value))}>
          {delayOptions.map((option) => (
            <option key={option.minutes} value={option.minutes}>
              {option.label}
            </option>
          ))}
        </select>
      </label>
      <p className="hint">1 min uses poc.delay.1m, 2 min uses poc.delay.2m, and 5 min uses poc.delay.5m.</p>
      <label>
        Message
        <textarea
          value={message}
          onChange={(event) => setMessage(event.target.value)}
          rows={4}
          maxLength={4000}
          required
        />
      </label>
      <label>
        Simulated processing (seconds)
        <input
          type="number"
          min={0}
          max={30}
          step={1}
          value={processing}
          onChange={(event) => setProcessing(Number(event.target.value))}
        />
      </label>
      <p className="hint">Processing time is only a simulation to show the business logic processing.</p>
      {shown ? <p className="banner error">{shown}</p> : null}
      <button type="submit" disabled={disabled}>
        {disabled ? "Publishing…" : "Publish Job"}
      </button>
    </form>
  );
}
