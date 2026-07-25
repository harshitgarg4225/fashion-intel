import { useCallback, useEffect, useRef, useState } from "react";
import { CaretLeft, CaretRight, DownloadSimple, Plus, ShareNetwork, X } from "@phosphor-icons/react";
import { OptimizedImage } from "./OptimizedImage.jsx";
import { deliverShare } from "./share-utils.js";
import { IMAGE_ACCEPT, prepareImageFile } from "./image-input.js";
import "./journal.css";

const LOG_FEELINGS = ["confident", "effortless", "cozy", "bold", "romantic", "casual"];

function formatRange(start, end) {
  const options = { month: "long", day: "numeric" };
  const from = new Date(`${start}T00:00:00`);
  const to = new Date(`${end}T00:00:00`);
  return `${from.toLocaleDateString(undefined, options)} — ${to.toLocaleDateString(undefined, options)}`;
}

function prettyDay(date) {
  return new Date(`${date}T00:00:00`).toLocaleDateString(undefined, { weekday: "long", month: "long", day: "numeric" });
}

function JournalLogger({ date, today, onClose, onSaved }) {
  const [photo, setPhoto] = useState(null);
  const [note, setNote] = useState("");
  const [mood, setMood] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const fileRef = useRef(null);

  const pickPhoto = async (event) => {
    const file = event.target.files?.[0];
    event.target.value = "";
    if (!file) return;
    setError("");
    try {
      setPhoto({ name: file.name, dataUrl: await prepareImageFile(file) });
    } catch (readError) {
      setError(readError.message);
    }
  };

  const save = async () => {
    if (!photo) {
      setError("Add a photo of what you wore — a mirror selfie works perfectly.");
      return;
    }
    setBusy(true);
    setError("");
    try {
      const response = await fetch("/api/journal/log", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ photoDataUrl: photo.dataUrl, date, note: note.trim(), mood }),
      });
      const value = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(value.error || "Could not save the entry.");
      onSaved();
    } catch (requestError) {
      setError(requestError.message);
      setBusy(false);
    }
  };

  return (
    <div className="journal-logger" role="dialog" aria-label="Log what you wore">
      <div className="journal-logger-head">
        <p className="journal-logger-title">{date === today ? "What did you wear today?" : `What did you wear on ${prettyDay(date)}?`}</p>
        <button type="button" className="journal-logger-close" onClick={onClose} aria-label="Close">
          <X size={16} aria-hidden="true" />
        </button>
      </div>
      <div className="journal-logger-body">
        <button type="button" className={`journal-photo-pick${photo ? " has-photo" : ""}`} onClick={() => fileRef.current?.click()}>
          {photo ? <img src={photo.dataUrl} alt="Your outfit today" /> : <span><Plus size={18} aria-hidden="true" /> Add a photo</span>}
        </button>
        <input ref={fileRef} type="file" accept={IMAGE_ACCEPT} hidden onChange={pickPhoto} />
        <div className="journal-logger-fields">
          <input
            value={note}
            onChange={(event) => setNote(event.target.value)}
            maxLength={280}
            placeholder="A note for future you — “first day at the internship”"
            aria-label="Journal note"
          />
          <div className="journal-feelings" role="group" aria-label="How did it feel?">
            {LOG_FEELINGS.map((feeling) => (
              <button
                key={feeling}
                type="button"
                className={`journal-feeling${mood === feeling ? " active" : ""}`}
                aria-pressed={mood === feeling}
                onClick={() => setMood((current) => current === feeling ? "" : feeling)}
              >
                {feeling}
              </button>
            ))}
          </div>
          <div className="journal-logger-actions">
            <button type="button" className="journal-collage" onClick={save} disabled={busy}>{busy ? "Saving…" : "Save to journal"}</button>
          </div>
          {error && <p className="journal-note journal-share-error" role="alert">{error}</p>}
          <p className="journal-note">A mirror photo is all it takes. Journaling never uses render credits.</p>
        </div>
      </div>
    </div>
  );
}

export function Journal({ initialLog = false }) {
  const [offset, setOffset] = useState(0);
  const [week, setWeek] = useState(null);
  const [error, setError] = useState("");
  const [copied, setCopied] = useState(false);
  const [shareError, setShareError] = useState("");
  const [manualShareUrl, setManualShareUrl] = useState(null);
  const today = new Date().toISOString().slice(0, 10);
  const [logDate, setLogDate] = useState(initialLog ? today : null);

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

  const removeEntry = async (id) => {
    if (!window.confirm("Remove this journal entry? The photo is deleted too.")) return;
    try {
      const response = await fetch(`/api/journal/log/${id}`, { method: "DELETE" });
      if (!response.ok) throw new Error("Could not remove the entry.");
      load(offset);
    } catch (requestError) {
      setError(requestError.message);
    }
  };

  if (error) return <p className="status error">{error}</p>;
  if (!week) return <p className="status">Opening your journal</p>;

  const hasWears = week.stats.totalWears > 0;
  const todayLogged = week.days.some((day) => day.date === today && day.looks.length > 0);

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
            <p className="journal-summary">Nothing here yet — snap what you wore, or tap “Wear it” on a styled look.</p>
          )}
        </div>
        <button type="button" className="journal-nav" onClick={() => setOffset((current) => Math.max(0, current - 1))} disabled={offset === 0} aria-label="Next week">
          <CaretRight size={16} aria-hidden="true" />
        </button>
      </div>

      {offset === 0 && !logDate && !todayLogged && (
        <div className="journal-log-cta">
          <button type="button" className="journal-collage" onClick={() => setLogDate(today)}>
            <Plus size={15} aria-hidden="true" /> Log what I wore today
          </button>
        </div>
      )}

      {logDate && (
        <JournalLogger
          date={logDate}
          today={today}
          onClose={() => setLogDate(null)}
          onSaved={() => { setLogDate(null); load(offset); }}
        />
      )}

      <div className="journal-week">
        {week.days.map((day) => {
          const look = day.looks[0];
          const isToday = day.date === today;
          const isFuture = day.date > today;
          return (
            <div className={`journal-day${isToday ? " is-today" : ""}`} key={day.date}>
              <span className="journal-day-label">{day.weekday.slice(0, 3)}</span>
              {look ? (
                <div className="journal-day-frame">
                  <OptimizedImage src={look.image} alt={`${day.weekday}: ${look.name}`} sizes="(max-width: 720px) 45vw, 170px" breakpoints={[160, 240, 340]} />
                </div>
              ) : (
                <button
                  type="button"
                  className="journal-day-frame empty"
                  disabled={isFuture}
                  onClick={() => setLogDate(day.date)}
                  aria-label={isFuture ? undefined : `Log what you wore on ${prettyDay(day.date)}`}
                  title={isFuture ? undefined : "Log what you wore"}
                >
                  <span className="journal-day-empty">{isFuture ? "" : isToday ? "log today" : "+"}</span>
                </button>
              )}
              {look && <span className="journal-day-name">{look.name}</span>}
              {look?.journal && (
                <button type="button" className="journal-day-remove" onClick={() => removeEntry(look.id)}>remove</button>
              )}
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
