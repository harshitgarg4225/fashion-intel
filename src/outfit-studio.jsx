import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ArrowCounterClockwise, ArrowsClockwise, CloudSun, CoatHanger, Heart, ImageSquare, Sparkle, SpinnerGap, ThumbsDown, ThumbsUp, Trash, WarningCircle, X } from "@phosphor-icons/react";
import { OptimizedImage } from "./OptimizedImage.jsx";
import { deliverShare } from "./share-utils.js";
import "./outfit-studio.css";

const API = "/api/outfits";
const ACTIVE_STATUSES = new Set(["curating", "rendering"]);

const MOODS = [
  { id: "confident", label: "Confident" },
  { id: "effortless", label: "Effortless" },
  { id: "sharp", label: "Sharp" },
  { id: "cozy", label: "Cozy" },
  { id: "playful", label: "Playful" },
  { id: "bold", label: "Bold" },
  { id: "romantic", label: "Romantic" },
  { id: "grounded", label: "Grounded" },
];

const OCCASION_PRESETS = ["Interview", "Date night", "Wedding guest", "Office", "Weekend", "Travel day"];
const DOWNVOTE_REASONS = ["too formal", "too bold", "not my colors", "wrong for weather", "doesn't feel like me"];

async function api(path, options) {
  const response = await fetch(path, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options?.headers || {}) },
  });
  const value = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(value.error || "The outfit request failed.");
  return value;
}

function describeWeatherCode(code) {
  if (code === 0) return "clear";
  if (code <= 3) return "partly cloudy";
  if (code <= 48) return "foggy";
  if (code <= 67) return "rainy";
  if (code <= 77) return "snowy";
  if (code <= 82) return "rain showers";
  return "stormy";
}

function getLocalWeather() {
  return new Promise((resolve, reject) => {
    if (!navigator.geolocation) return reject(new Error("Location is not available in this browser."));
    navigator.geolocation.getCurrentPosition(
      async (position) => {
        try {
          const { latitude, longitude } = position.coords;
          const response = await fetch(`https://api.open-meteo.com/v1/forecast?latitude=${latitude.toFixed(3)}&longitude=${longitude.toFixed(3)}&current=temperature_2m,precipitation,weather_code`);
          if (!response.ok) throw new Error("Weather lookup failed.");
          const data = await response.json();
          const current = data.current || {};
          if (!Number.isFinite(current.temperature_2m)) throw new Error("Weather lookup failed.");
          resolve(`${Math.round(current.temperature_2m)}°C and ${describeWeatherCode(current.weather_code ?? 0)}`);
        } catch (error) {
          reject(error);
        }
      },
      () => reject(new Error("Location permission was declined.")),
      { timeout: 8000, maximumAge: 600000 },
    );
  });
}

const fileToDataUrl = (file) => new Promise((resolve, reject) => {
  const reader = new FileReader();
  reader.onload = () => resolve(reader.result);
  reader.onerror = () => reject(reader.error || new Error("Could not read that image."));
  reader.readAsDataURL(file);
});

function statusCopy(outfit) {
  if (outfit.status === "curating") return "Styling the combination";
  if (outfit.status === "rendering") return "Rendering the modeled photo";
  if (outfit.status === "failed") return outfit.error || "Generation failed";
  return null;
}

function StyleProfilePanel() {
  const [profile, setProfile] = useState(null);
  const [saved, setSaved] = useState(false);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api("/api/profile").then(setProfile).catch(() => setProfile({ styleNotes: "", hardRules: "" }));
  }, []);

  if (!profile) return null;

  const save = async () => {
    setSaving(true);
    try {
      setProfile(await api("/api/profile", { method: "PUT", body: JSON.stringify(profile) }));
      setSaved(true);
      setTimeout(() => setSaved(false), 2500);
    } catch {} finally {
      setSaving(false);
    }
  };

  return (
    <details className="style-profile">
      <summary>Style profile</summary>
      <p className="stylist-note">The stylist reads this on every look. Notes shape choices; hard rules are never violated.</p>
      <label>
        <span>Style notes</span>
        <textarea rows="2" value={profile.styleNotes} placeholder="e.g. prefers earth tones, loves texture, minimal branding" onChange={(event) => setProfile((current) => ({ ...current, styleNotes: event.target.value }))} />
      </label>
      <label>
        <span>Hard rules</span>
        <textarea rows="2" value={profile.hardRules} placeholder="e.g. never crop tops, no logos at work, always covered shoulders" onChange={(event) => setProfile((current) => ({ ...current, hardRules: event.target.value }))} />
      </label>
      <button type="button" className="outfit-create profile-save" onClick={save} disabled={saving}>{saved ? "Saved" : "Save profile"}</button>
    </details>
  );
}

function VerdictControls({ outfit, onVerdict }) {
  const [pickingReason, setPickingReason] = useState(false);

  if (pickingReason) {
    return (
      <div className="verdict-reasons" role="group" aria-label="Why didn't this work?">
        {DOWNVOTE_REASONS.map((reason) => (
          <button key={reason} type="button" onClick={() => { onVerdict("down", reason); setPickingReason(false); }}>{reason}</button>
        ))}
        <button type="button" className="verdict-skip" onClick={() => { onVerdict("down", null); setPickingReason(false); }}>skip</button>
      </div>
    );
  }

  return (
    <div className="verdict-row">
      <button
        type="button"
        className={`verdict-button${outfit.verdict === "up" ? " active" : ""}`}
        onClick={() => onVerdict(outfit.verdict === "up" ? null : "up", null)}
        aria-pressed={outfit.verdict === "up"}
        aria-label="This look worked"
      >
        <ThumbsUp size={15} weight={outfit.verdict === "up" ? "fill" : "regular"} aria-hidden="true" />
      </button>
      <button
        type="button"
        className={`verdict-button${outfit.verdict === "down" ? " active down" : ""}`}
        onClick={() => outfit.verdict === "down" ? onVerdict(null, null) : setPickingReason(true)}
        aria-pressed={outfit.verdict === "down"}
        aria-label="This look didn't work"
      >
        <ThumbsDown size={15} weight={outfit.verdict === "down" ? "fill" : "regular"} aria-hidden="true" />
      </button>
      {outfit.verdict === "down" && outfit.verdictReason && <span className="verdict-reason-label">{outfit.verdictReason}</span>}
    </div>
  );
}

function OutfitCard({ outfit, itemsById, onRetry, onDelete, onToggleFavorite, onWear, onVerdict, onVariation, onRemix, onShare, sharedId }) {
  const active = ACTIVE_STATUSES.has(outfit.status);
  const garments = (outfit.garmentIds || []).map((id) => itemsById.get(id)).filter(Boolean);
  const wearCount = outfit.wornAt?.length || 0;

  return (
    <article className={`outfit-card status-${outfit.status}`}>
      <div className="outfit-photo">
        {outfit.status === "ready" && outfit.image ? (
          <OptimizedImage
            src={outfit.image}
            alt={`Modeled photo of the outfit ${outfit.name}`}
            sizes="(max-width: 720px) 100vw, 380px"
            breakpoints={[320, 480, 640, 800]}
          />
        ) : (
          <div className={`outfit-placeholder${outfit.status === "failed" ? " failed" : ""}`} role="status">
            {active ? <SpinnerGap className="spin" size={28} aria-hidden="true" /> : <WarningCircle size={28} aria-hidden="true" />}
            <p>{statusCopy(outfit)}</p>
          </div>
        )}
        {outfit.status === "ready" && (
          <button
            type="button"
            className={`outfit-favorite${outfit.favorite ? " is-favorite" : ""}`}
            onClick={() => onToggleFavorite(outfit)}
            aria-label={outfit.favorite ? `Remove ${outfit.name} from favorites` : `Add ${outfit.name} to favorites`}
            aria-pressed={Boolean(outfit.favorite)}
          >
            <Heart size={18} weight={outfit.favorite ? "fill" : "regular"} aria-hidden="true" />
          </button>
        )}
      </div>
      <div className="outfit-body">
        <div className="outfit-heading">
          <h3>{outfit.name}</h3>
          {(outfit.mood || !!outfit.occasion?.length || wearCount > 0) && (
            <div className="occasion-row">
              {outfit.mood && <span className="occasion-chip mood-chip">feels {outfit.mood}</span>}
              {(outfit.occasion || []).map((label) => <span className="occasion-chip" key={label}>{label}</span>)}
              {wearCount > 0 && <span className="occasion-chip worn-chip">worn {wearCount}×</span>}
            </div>
          )}
        </div>
        {outfit.reason && <p className="outfit-reason">{outfit.reason}</p>}
        {!!garments.length && (
          <div className="outfit-garments" aria-label="Garments in this outfit">
            {garments.map((item) => (
              <span className="outfit-garment" key={item.id} title={item.name}>
                <OptimizedImage src={item.thumbnail || item.image} alt={item.name} sizes="44px" breakpoints={[44, 88]} />
              </span>
            ))}
          </div>
        )}
        {outfit.status === "ready" && <VerdictControls outfit={outfit} onVerdict={(verdict, reason) => onVerdict(outfit, verdict, reason)} />}
        <div className="outfit-actions">
          {outfit.status === "ready" && (
            <>
              <button type="button" className="outfit-wear" onClick={() => onWear(outfit.id)}>
                <CoatHanger size={14} aria-hidden="true" /> Wear it
              </button>
              <button type="button" className="outfit-retry" onClick={() => onVariation(outfit)}>
                <ArrowsClockwise size={14} aria-hidden="true" /> Another take
              </button>
              {garments.some((item) => item.part === "upperbody") && (
                <button type="button" className="outfit-swap" onClick={() => onRemix(outfit.id, "top")}>Swap top</button>
              )}
              {garments.some((item) => item.part === "lowerbody") && (
                <button type="button" className="outfit-swap" onClick={() => onRemix(outfit.id, "bottom")}>Swap bottom</button>
              )}
              <button type="button" className="outfit-swap" onClick={() => onShare(outfit)} title="Create a public share link for this look">{sharedId === outfit.id ? "Link copied" : "Share"}</button>
            </>
          )}
          {outfit.status === "failed" && (
            <button type="button" className="outfit-retry" onClick={() => onRetry(outfit.id)}>
              <ArrowCounterClockwise size={14} aria-hidden="true" /> Retry
            </button>
          )}
          <span className="action-spacer" />
          <button type="button" className="outfit-delete" onClick={() => onDelete(outfit.id)} disabled={active} aria-label={`Delete ${outfit.name}`}>
            <Trash size={14} aria-hidden="true" /> Delete
          </button>
        </div>
      </div>
    </article>
  );
}

export function OutfitStudio({ items, initialMood = null }) {
  const [outfits, setOutfits] = useState([]);
  const [config, setConfig] = useState(null);
  const [usage, setUsage] = useState(null);
  const [mood, setMood] = useState(initialMood);
  const [customMood, setCustomMood] = useState("");
  const [context, setContext] = useState("");
  const [weather, setWeather] = useState(null);
  const [weatherBusy, setWeatherBusy] = useState(false);
  const [inspiration, setInspiration] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const pollRef = useRef(null);
  const inspirationInputRef = useRef(null);

  const itemsById = useMemo(() => new Map(items.map((item) => [item.id, item])), [items]);
  const hasActive = outfits.some((outfit) => ACTIVE_STATUSES.has(outfit.status));
  const chosenMood = customMood.trim() || mood;

  const prevStatusesRef = useRef(new Map());

  const refresh = useCallback(async () => {
    try {
      const [loadedOutfits, loadedConfig, loadedUsage] = await Promise.all([api(API), api(`${API}/config`), api("/api/usage").catch(() => null)]);
      const previous = prevStatusesRef.current;
      for (const outfit of loadedOutfits) {
        if (outfit.status === "ready" && ACTIVE_STATUSES.has(previous.get(outfit.id)) && document.hidden && typeof Notification !== "undefined" && Notification.permission === "granted") {
          try { new Notification("Your look is ready", { body: outfit.name, tag: outfit.id }); } catch {}
        }
      }
      prevStatusesRef.current = new Map(loadedOutfits.map((outfit) => [outfit.id, outfit.status]));
      setOutfits(loadedOutfits);
      setConfig(loadedConfig);
      setUsage(loadedUsage);
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  useEffect(() => {
    if (!hasActive) return undefined;
    pollRef.current = setInterval(refresh, 2500);
    return () => clearInterval(pollRef.current);
  }, [hasActive, refresh]);

  const toggleWeather = async () => {
    if (weather) { setWeather(null); return; }
    setWeatherBusy(true);
    setError("");
    try {
      setWeather(await getLocalWeather());
    } catch (weatherError) {
      setError(weatherError.message);
    } finally {
      setWeatherBusy(false);
    }
  };

  const pickInspiration = async (event) => {
    const file = event.target.files?.[0];
    event.target.value = "";
    if (!file || !file.type.startsWith("image/")) return;
    try {
      setInspiration({ name: file.name, dataUrl: await fileToDataUrl(file) });
    } catch (readError) {
      setError(readError.message);
    }
  };

  const createLook = async ({ lookMood = chosenMood, lookDirection = null, count = 1 } = {}) => {
    setCreating(true);
    setError("");
    if (typeof Notification !== "undefined" && Notification.permission === "default") {
      Notification.requestPermission().catch(() => {});
    }
    try {
      const direction = lookDirection ?? [context.trim(), weather ? `today's weather is ${weather}` : ""].filter(Boolean).join("; ");
      for (let index = 0; index < count; index += 1) {
        const record = await api(API, {
          method: "POST",
          body: JSON.stringify({
            mood: lookMood || undefined,
            direction: direction || undefined,
            inspirationDataUrl: index === 0 ? inspiration?.dataUrl : undefined,
          }),
        });
        setOutfits((current) => [...current, record]);
      }
      if (inspiration) setInspiration(null);
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setCreating(false);
    }
  };

  const retryOutfit = async (id) => {
    setError("");
    try {
      const record = await api(`${API}/${id}/retry`, { method: "POST" });
      setOutfits((current) => current.map((outfit) => outfit.id === id ? record : outfit));
    } catch (requestError) {
      setError(requestError.message);
    }
  };

  const deleteOutfit = async (id) => {
    setError("");
    try {
      await api(`${API}/${id}`, { method: "DELETE" });
      setOutfits((current) => current.filter((outfit) => outfit.id !== id));
    } catch (requestError) {
      setError(requestError.message);
    }
  };

  const patchOutfit = async (id, patch) => {
    setError("");
    try {
      const record = await api(`${API}/${id}`, { method: "PATCH", body: JSON.stringify(patch) });
      setOutfits((current) => current.map((entry) => entry.id === id ? record : entry));
    } catch (requestError) {
      setError(requestError.message);
    }
  };

  const [sharedId, setSharedId] = useState(null);
  const [manualShareUrl, setManualShareUrl] = useState(null);

  const shareOutfit = async (outfit) => {
    setError("");
    setManualShareUrl(null);
    try {
      const share = await api(`${API}/${outfit.id}/share`, { method: "POST" });
      const shareUrl = `${window.location.origin}${share.path}`;
      const delivery = await deliverShare({ title: `${outfit.name} — styled by Mira`, url: shareUrl });
      if (delivery.status === "shared" || delivery.status === "copied") {
        setSharedId(outfit.id);
        setTimeout(() => setSharedId(null), 2500);
      } else if (delivery.status === "manual") {
        setManualShareUrl(shareUrl);
      }
    } catch (requestError) {
      setError(requestError.message);
    }
  };

  const remixOutfit = async (id, slot) => {
    setError("");
    try {
      const record = await api(`${API}/${id}/remix`, { method: "POST", body: JSON.stringify({ slot }) });
      setOutfits((current) => [...current, record]);
    } catch (requestError) {
      setError(requestError.message);
    }
  };

  const wearOutfit = async (id) => {
    setError("");
    try {
      const record = await api(`${API}/${id}/wear`, { method: "POST" });
      setOutfits((current) => current.map((entry) => entry.id === id ? record : entry));
    } catch (requestError) {
      setError(requestError.message);
    }
  };

  const setupMessage = useMemo(() => {
    if (!config || config.ready) return null;
    const missing = [
      !config.hasOpenAIKey && `add ${{ gemini: "GEMINI_API_KEY", fal: "FAL_API_KEY" }[config.imageProvider] || "OPENAI_API_KEY"} to .env`,
      !config.hasStylistKey && `add an API key for the ${config.stylistProvider} stylist`,
      !config.hasModelReference && "add your reference photo (the import tray can capture one)",
      (!config.tops || !config.bottoms) && "import at least one top and one bottom",
    ].filter(Boolean);
    return `To generate outfits, ${missing.join(", ")}.`;
  }, [config]);

  const sortedOutfits = useMemo(
    () => [...outfits].sort((a, b) => {
      if (Boolean(a.favorite) !== Boolean(b.favorite)) return a.favorite ? -1 : 1;
      return (b.createdAt || "").localeCompare(a.createdAt || "");
    }),
    [outfits],
  );

  return (
    <section className="outfit-studio" aria-label="Outfit studio">
      <div className="mood-composer">
        <h2 className="mood-question">How do you want to feel today?</h2>
        <div className="mood-chips" role="group" aria-label="Pick a feeling">
          {MOODS.map((entry) => (
            <button
              key={entry.id}
              type="button"
              className={`mood-chip-button${mood === entry.id && !customMood.trim() ? " active" : ""}`}
              onClick={() => { setMood((current) => current === entry.id ? null : entry.id); setCustomMood(""); }}
              aria-pressed={mood === entry.id && !customMood.trim()}
            >
              {entry.label}
            </button>
          ))}
        </div>
        <div className="mood-inputs">
          <input
            value={customMood}
            onChange={(event) => setCustomMood(event.target.value)}
            placeholder="…or describe the feeling in your own words"
            aria-label="Describe the feeling in your own words"
          />
          <input
            value={context}
            onChange={(event) => setContext(event.target.value)}
            placeholder="Occasion or constraints, e.g. dinner with friends"
            aria-label="Occasion or constraints"
          />
        </div>
        <div className="occasion-presets" role="group" aria-label="Occasion presets">
          {OCCASION_PRESETS.map((preset) => (
            <button key={preset} type="button" className={`preset-chip${context === preset ? " active" : ""}`} onClick={() => setContext((current) => current === preset ? "" : preset)}>{preset}</button>
          ))}
        </div>
        <div className="mood-actions">
          <button type="button" className={`weather-toggle${weather ? " active" : ""}`} onClick={toggleWeather} disabled={weatherBusy} aria-pressed={Boolean(weather)}>
            {weatherBusy ? <SpinnerGap className="spin" size={15} aria-hidden="true" /> : <CloudSun size={15} aria-hidden="true" />}
            {weather ? `Dressing for ${weather}` : "Use my weather"}
          </button>
          <button type="button" className={`weather-toggle${inspiration ? " active" : ""}`} onClick={() => inspiration ? setInspiration(null) : inspirationInputRef.current?.click()}>
            {inspiration ? <X size={15} aria-hidden="true" /> : <ImageSquare size={15} aria-hidden="true" />}
            {inspiration ? `Inspired by ${inspiration.name.slice(0, 18)}` : "Inspiration photo"}
          </button>
          <input ref={inspirationInputRef} type="file" accept="image/*" hidden onChange={pickInspiration} />
          <button type="button" className="outfit-create" onClick={() => createLook()} disabled={creating || (config && !config.ready)}>
            {creating ? <SpinnerGap className="spin" size={15} aria-hidden="true" /> : <Sparkle size={15} aria-hidden="true" />}
            {chosenMood ? `Dress me ${customMood.trim() ? "for that" : chosenMood}` : "Surprise me"}
          </button>
          <button type="button" className="weather-toggle" onClick={() => createLook({ count: 3 })} disabled={creating || hasActive || (config && !config.ready)} title="Generate three candidate looks to choose from">
            3 options
          </button>
        </div>
        {config?.stylistProvider && (
          <p className="stylist-note">
            Stylist runs on {{ anthropic: "Claude", gemini: "Gemini", openai: "OpenAI" }[config.stylistProvider] || "OpenAI"}; photos render with {{ gemini: "Gemini", fal: "Seedream (fal.ai)", openai: "gpt-image" }[config.imageProvider] || "gpt-image"}.
            {usage?.today && ` Today: ${usage.today.imageCalls} renders, ≈$${usage.today.estCostUsd.toFixed(2)}${usage.budgetUsd ? ` of $${usage.budgetUsd.toFixed(2)} budget` : ""}.`}
          </p>
        )}
        <StyleProfilePanel />
      </div>

      {setupMessage && <p className="status">{setupMessage}</p>}
      {manualShareUrl && <p className="status manual-share">Your public link: <a href={manualShareUrl} target="_blank" rel="noreferrer">{manualShareUrl}</a></p>}
      {error && <p className="status error">{error}</p>}
      {!error && loading && <p className="status">Loading outfits</p>}
      {!loading && !sortedOutfits.length && !setupMessage && (
        <p className="status empty">No looks yet. Pick a feeling and let the stylist dress you from your own closet.</p>
      )}

      {!!sortedOutfits.length && (
        <div className="outfit-grid">
          {sortedOutfits.map((outfit) => (
            <OutfitCard
              key={outfit.id}
              outfit={outfit}
              itemsById={itemsById}
              onRetry={retryOutfit}
              onDelete={deleteOutfit}
              onToggleFavorite={(entry) => patchOutfit(entry.id, { favorite: !entry.favorite })}
              onWear={wearOutfit}
              onVerdict={(entry, verdict, reason) => patchOutfit(entry.id, { verdict, verdictReason: reason })}
              onVariation={(entry) => createLook({ lookMood: entry.mood, lookDirection: entry.direction || "", count: 1 })}
              onRemix={remixOutfit}
              onShare={shareOutfit}
              sharedId={sharedId}
            />
          ))}
        </div>
      )}
    </section>
  );
}
