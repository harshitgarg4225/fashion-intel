import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ArrowCounterClockwise, Sparkle, SpinnerGap, Trash, WarningCircle } from "@phosphor-icons/react";
import { OptimizedImage } from "./OptimizedImage.jsx";
import "./outfit-studio.css";

const API = "/api/outfits";
const ACTIVE_STATUSES = new Set(["curating", "rendering"]);

async function api(path, options) {
  const response = await fetch(path, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options?.headers || {}) },
  });
  const value = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(value.error || "The outfit request failed.");
  return value;
}

function statusCopy(outfit) {
  if (outfit.status === "curating") return "Styling the combination";
  if (outfit.status === "rendering") return "Rendering the modeled photo";
  if (outfit.status === "failed") return outfit.error || "Generation failed";
  return null;
}

function OutfitCard({ outfit, itemsById, onRetry, onDelete }) {
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
      </div>
      <div className="outfit-body">
        <div className="outfit-heading">
          <h3>{outfit.name}</h3>
          {!!outfit.occasion?.length && (
            <div className="occasion-row">
              {outfit.occasion.map((label) => <span className="occasion-chip" key={label}>{label}</span>)}
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
  const [direction, setDirection] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const pollRef = useRef(null);

  const itemsById = useMemo(() => new Map(items.map((item) => [item.id, item])), [items]);
  const hasActive = outfits.some((outfit) => ACTIVE_STATUSES.has(outfit.status));

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

  const createOutfit = async () => {
    setCreating(true);
    setError("");
    try {
      const record = await api(API, { method: "POST", body: JSON.stringify({ direction: direction.trim() || undefined }) });
      setOutfits((current) => [...current, record]);
      setDirection("");
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
    () => [...outfits].sort((a, b) => (b.createdAt || "").localeCompare(a.createdAt || "")),
    [outfits],
  );

  return (
    <section className="outfit-studio" aria-label="Outfit studio">
      <div className="outfit-composer">
        <input
          value={direction}
          onChange={(event) => setDirection(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !creating && config?.ready) createOutfit();
          }}
          placeholder="Optional direction, e.g. smart-casual dinner, warm evening"
          aria-label="Styling direction for the new outfit"
          disabled={creating}
        />
        <button type="button" className="outfit-create" onClick={createOutfit} disabled={creating || (config && !config.ready)}>
          {creating ? <SpinnerGap className="spin" size={15} aria-hidden="true" /> : <Sparkle size={15} aria-hidden="true" />}
          New look
        </button>
      </div>

      {config?.stylistProvider && (
        <p className="stylist-note">Stylist runs on {config.stylistProvider === "anthropic" ? "Claude" : "OpenAI"}; photos render with gpt-image.</p>
      )}
      {setupMessage && <p className="status">{setupMessage}</p>}
      {error && <p className="status error">{error}</p>}
      {!error && loading && <p className="status">Loading outfits</p>}
      {!loading && !sortedOutfits.length && !setupMessage && (
        <p className="status empty">No looks yet. Generate your first outfit from the pieces in your closet.</p>
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
            />
          ))}
        </div>
      )}
    </section>
  );
}
