import { useCallback, useEffect, useState } from "react";
import { CaretLeft, CaretRight, DownloadSimple, ShareNetwork } from "@phosphor-icons/react";
import { OptimizedImage } from "./OptimizedImage.jsx";
import { deliverShare } from "./share-utils.js";
import "./journal.css";

function formatRange(start, end) {
  const options = { month: "long", day: "numeric" };
  const from = new Date(`${start}T00:00:00`);
  const to = new Date(`${end}T00:00:00`);
  return `${from.toLocaleDateString(undefined, options)} — ${to.toLocaleDateString(undefined, options)}`;
}

export function Journal() {
  const [offset, setOffset] = useState(0);
  const [week, setWeek] = useState(null);
  const [error, setError] = useState("");
  const [copied, setCopied] = useState(false);
  const [shareError, setShareError] = useState("");
  const [manualShareUrl, setManualShareUrl] = useState(null);
  const today = new Date().toISOString().slice(0, 10);

  const load = useCallback(async (weekOffset) => {
    setError("");
    try {
      const response = await fetch(`/api/review/week?offset=${weekOffset}`, { cache: "no-store" });
      const value = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(value.error || "Could not load the week.");
      setWeek(value);
    } catch (requestError) {
      setError(requestError.message);
    }
  }, []);

  useEffect(() => { load(offset); }, [offset, load]);

  if (error) return <p className="status error">{error}</p>;
  if (!week) return <p className="status">Opening your journal</p>;

  const hasWears = week.stats.totalWears > 0;

  return (
    <section className="journal" aria-label="Wear journal">
      <div className="journal-header">
        <button type="button" className="journal-nav" onClick={() => setOffset((current) => Math.min(52, current + 1))} aria-label="Previous week">
          <CaretLeft size={16} aria-hidden="true" />
        </button>
        <div className="journal-title">
          <p className="journal-eyebrow">{offset === 0 ? "This week" : offset === 1 ? "Last week" : `${offset} weeks ago`}</p>
          <h2>{formatRange(week.start, week.end)}</h2>
          {hasWears ? (
            <p className="journal-summary">
              {week.stats.streak > 1 && <span className="streak-chip">{week.stats.streak}-day streak</span>}
              Dressed {week.stats.daysDressed} of 7 days
              {week.stats.topPiece && <> · most worn: <em>{week.stats.topPiece.name}</em></>}
              {!!week.stats.moods.length && <> · felt <em>{week.stats.moods.join(", ")}</em></>}
            </p>
          ) : (
            <p className="journal-summary">No looks logged{offset === 0 ? " yet — tap “Wear it” on today's look" : " this week"}.</p>
          )}
        </div>
        <button type="button" className="journal-nav" onClick={() => setOffset((current) => Math.max(0, current - 1))} disabled={offset === 0} aria-label="Next week">
          <CaretRight size={16} aria-hidden="true" />
        </button>
      </div>

      <div className="journal-week">
        {week.days.map((day) => {
          const look = day.looks[0];
          const isToday = day.date === today;
          const isFuture = day.date > today;
          return (
            <div className={`journal-day${isToday ? " is-today" : ""}`} key={day.date}>
              <span className="journal-day-label">{day.weekday.slice(0, 3)}</span>
              <div className={`journal-day-frame${look ? "" : " empty"}`}>
                {look ? (
                  <OptimizedImage src={look.image} alt={`${day.weekday}: ${look.name}`} sizes="(max-width: 720px) 45vw, 170px" breakpoints={[160, 240, 340]} />
                ) : (
                  <span className="journal-day-empty">{isFuture ? "" : isToday ? "today" : "—"}</span>
                )}
              </div>
              {look && <span className="journal-day-name">{look.name}</span>}
              {day.looks.length > 1 && <span className="journal-day-extra">+{day.looks.length - 1} more</span>}
            </div>
          );
        })}
      </div>

      {hasWears && (
        <div className="journal-actions">
          <button
            type="button"
            className="journal-collage"
            onClick={async () => {
              setShareError("");
              setManualShareUrl(null);
              try {
                const response = await fetch("/api/review/week/share", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ offset }) });
                const share = await response.json().catch(() => ({}));
                if (!response.ok) throw new Error(share.error || "Could not create the share link.");
                const shareUrl = `${window.location.origin}${share.path}`;
                const delivery = await deliverShare({ title: "My week in looks — Mira", url: shareUrl });
                if (delivery.status === "shared" || delivery.status === "copied") {
                  setCopied(true);
                  setTimeout(() => setCopied(false), 2500);
                } else if (delivery.status === "manual") {
                  setManualShareUrl(shareUrl);
                }
              } catch (requestError) {
                setShareError(requestError.message);
              }
            }}
          >
            <ShareNetwork size={15} aria-hidden="true" /> {copied ? "Link copied" : "Share my week"}
          </button>
          <a className="journal-download" href={`/api/review/week/collage.png?offset=${offset}`} target="_blank" rel="noreferrer">
            <DownloadSimple size={14} aria-hidden="true" /> Download collage
          </a>
          {manualShareUrl && <p className="journal-note">Your public link: <a href={manualShareUrl} target="_blank" rel="noreferrer">{manualShareUrl}</a></p>}
          {shareError && <p className="journal-note journal-share-error" role="alert">{shareError}</p>}
          <p className="journal-note">Anyone with the link sees a beautiful page of your week — and can meet Mira from it.</p>
        </div>
      )}
    </section>
  );
}
