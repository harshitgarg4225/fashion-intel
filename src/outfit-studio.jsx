import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ArrowCounterClockwise, CloudSun, Heart, Sparkle, SpinnerGap, Trash, WarningCircle } from "@phosphor-icons/react";
import { OptimizedImage } from "./OptimizedImage.jsx";
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

function statusCopy(outfit) {
  if (outfit.status === "curating") return "Styling the combination";
  if (outfit.status === "rendering") return "Rendering the modeled photo";
  if (outfit.status === "failed") return outfit.error || "Generation failed";
  return null;
}

function OutfitCard({ outfit, itemsById, onRetry, onDelete, onToggleFavorite }) {
  const active = ACTIVE_STATUSES.has(outfit.status);
  const garments = (outfit.garmentIds || []).map((id) => itemsById.get(id)).filter(Boolean);

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
          {(outfit.mood || !!outfit.occasion?.length) && (
            <div className="occasion-row">
              {outfit.mood && <span className="occasion-chip mood-chip">feels {outfit.mood}</span>}
              {(outfit.occasion || []).map((label) => <span className="occasion-chip" key={label}>{label}</span>)}
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
        <div className="outfit-actions">
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

export function OutfitStudio({ items }) {
  const [outfits, setOutfits] = useState([]);
  const [config, setConfig] = useState(null);
  const [mood, setMood] = useState(null);
  const [customMood, setCustomMood] = useState("");
  const [context, setContext] = useState("");
  const [weather, setWeather] = useState(null);
  const [weatherBusy, setWeatherBusy] = useState(false);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const pollRef = useRef(null);

  const itemsById = useMemo(() => new Map(items.map((item) => [item.id, item])), [items]);
  const hasActive = outfits.some((outfit) => ACTIVE_STATUSES.has(outfit.status));
  const chosenMood = customMood.trim() || mood;

  const refresh = useCallback(async () => {
    try {
      const [loadedOutfits, loadedConfig] = await Promise.all([api(API), api(`${API}/config`)]);
      setOutfits(loadedOutfits);
      setConfig(loadedConfig);
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

  const createOutfit = async () => {
    setCreating(true);
    setError("");
    try {
      const direction = [context.trim(), weather ? `today's weather is ${weather}` : ""].filter(Boolean).join("; ");
      const record = await api(API, {
        method: "POST",
        body: JSON.stringify({ mood: chosenMood || undefined, direction: direction || undefined }),
      });
      setOutfits((current) => [...current, record]);
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

  const toggleFavorite = async (outfit) => {
    setError("");
    try {
      const record = await api(`${API}/${outfit.id}`, { method: "PATCH", body: JSON.stringify({ favorite: !outfit.favorite }) });
      setOutfits((current) => current.map((entry) => entry.id === outfit.id ? record : entry));
    } catch (requestError) {
      setError(requestError.message);
    }
  };

  const setupMessage = useMemo(() => {
    if (!config || config.ready) return null;
    const missing = [
      !config.hasOpenAIKey && "add OPENAI_API_KEY to .env",
      !config.hasStylistKey && `add an API key for the ${config.stylistProvider} stylist`,
      !config.hasModelReference && "add a PNG photo of yourself at data/model-reference.png",
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
        <div className="mood-actions">
          <button type="button" className={`weather-toggle${weather ? " active" : ""}`} onClick={toggleWeather} disabled={weatherBusy} aria-pressed={Boolean(weather)}>
            {weatherBusy ? <SpinnerGap className="spin" size={15} aria-hidden="true" /> : <CloudSun size={15} aria-hidden="true" />}
            {weather ? `Dressing for ${weather}` : "Use my weather"}
          </button>
          <button type="button" className="outfit-create" onClick={createOutfit} disabled={creating || (config && !config.ready)}>
            {creating ? <SpinnerGap className="spin" size={15} aria-hidden="true" /> : <Sparkle size={15} aria-hidden="true" />}
            {chosenMood ? `Dress me ${customMood.trim() ? "for that" : chosenMood}` : "Surprise me"}
          </button>
        </div>
        {config?.stylistProvider && (
          <p className="stylist-note">Stylist runs on {config.stylistProvider === "anthropic" ? "Claude" : "OpenAI"}; photos render with gpt-image. Weather stays on your device except the line added to your request.</p>
        )}
      </div>

      {setupMessage && <p className="status">{setupMessage}</p>}
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
              onToggleFavorite={toggleFavorite}
            />
          ))}
        </div>
      )}
    </section>
  );
}
