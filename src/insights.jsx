import { useEffect, useMemo, useState } from "react";
import { SpinnerGap } from "@phosphor-icons/react";
import { OptimizedImage } from "./OptimizedImage.jsx";
import "./insights.css";

const TYPE_LABELS = [
  ["upperbody", "Tops"],
  ["wholebody_up", "Jackets"],
  ["lowerbody", "Bottoms"],
  ["accessories_up", "Accessories"],
  ["shoes", "Shoes"],
];

function CapsulePlanner({ items }) {
  const [days, setDays] = useState("");
  const [destination, setDestination] = useState("");
  const [notes, setNotes] = useState("");
  const [plan, setPlan] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const itemsById = useMemo(() => new Map(items.map((item) => [item.id, item])), [items]);

  const submit = async () => {
    setBusy(true);
    setError("");
    setPlan(null);
    try {
      const response = await fetch("/api/capsule", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ days: Number(days), destination, notes }),
      });
      const value = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(value.error || "Could not plan the capsule.");
      setPlan(value);
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="insight-block capsule-planner">
      <h3>Pack for a trip</h3>
      <div className="capsule-form">
        <input type="number" min="1" max="21" value={days} placeholder="Days" aria-label="Trip length in days" onChange={(event) => setDays(event.target.value)} />
        <input value={destination} placeholder="Destination (optional)" aria-label="Destination" onChange={(event) => setDestination(event.target.value)} />
        <input value={notes} placeholder="Notes, e.g. beach + two dinners" aria-label="Trip notes" onChange={(event) => setNotes(event.target.value)} />
        <button type="button" disabled={busy || !Number(days)} onClick={submit}>{busy ? <SpinnerGap className="capsule-spin" size={14} /> : "Plan capsule"}</button>
      </div>
      {error && <p className="status error">{error}</p>}
      {plan && (
        <div className="capsule-result">
          <p className="capsule-rationale">{plan.rationale}</p>
          <div className="outfit-garments" aria-label="Capsule pieces">
            {plan.pieceIds.map((id) => {
              const item = itemsById.get(id);
              return item ? (
                <span className="outfit-garment" key={id} title={item.name}>
                  <OptimizedImage src={item.thumbnail || item.image} alt={item.name} sizes="44px" breakpoints={[44, 88]} />
                </span>
              ) : null;
            })}
          </div>
          <ol className="capsule-days">
            {plan.dayPlans.map((entry) => <li key={entry.day}>{entry.description}</li>)}
          </ol>
        </div>
      )}
    </div>
  );
}

export function Insights({ items }) {
  const [outfits, setOutfits] = useState([]);

  useEffect(() => {
    fetch("/api/outfits", { cache: "no-store" })
      .then((response) => response.ok ? response.json() : [])
      .then(setOutfits)
      .catch(() => {});
  }, []);

  const stats = useMemo(() => {
    const counts = Object.fromEntries(TYPE_LABELS.map(([id]) => [id, 0]));
    const colorCounts = new Map();
    for (const item of items) {
      counts[item.part] = (counts[item.part] || 0) + 1;
      const color = item.color?.toLowerCase();
      if (color) colorCounts.set(color, (colorCounts.get(color) || 0) + 1);
    }

    const wearByGarment = new Map();
    let totalWears = 0;
    for (const outfit of outfits) {
      const wears = outfit.wornAt?.length || 0;
      totalWears += wears;
      if (!wears) continue;
      for (const garmentId of outfit.garmentIds || []) {
        wearByGarment.set(garmentId, (wearByGarment.get(garmentId) || 0) + wears);
      }
    }

    const byId = new Map(items.map((item) => [item.id, item]));
    const mostWorn = [...wearByGarment.entries()]
      .map(([id, wears]) => ({ item: byId.get(id), wears }))
      .filter((entry) => entry.item)
      .sort((a, b) => b.wears - a.wears)
      .slice(0, 6);
    const neverWorn = items.filter((item) => !wearByGarment.has(item.id));

    const costPerWear = items
      .filter((item) => Number.isFinite(item.price) && item.price > 0)
      .map((item) => ({ item, wears: wearByGarment.get(item.id) || 0, cpw: item.price / Math.max(1, wearByGarment.get(item.id) || 0) }))
      .sort((a, b) => b.cpw - a.cpw)
      .slice(0, 6);

    const tops = counts.upperbody || 0;
    const bottoms = counts.lowerbody || 0;
    let gapAdvice = null;
    if (tops && bottoms) {
      if (tops >= bottoms * 3) gapAdvice = `You have ${tops} tops but only ${bottoms} ${bottoms === 1 ? "bottom" : "bottoms"} — one more bottom unlocks ${tops} new top-and-bottom pairings.`;
      else if (bottoms >= tops * 3) gapAdvice = `You have ${bottoms} bottoms but only ${tops} ${tops === 1 ? "top" : "tops"} — one more top unlocks ${bottoms} new pairings.`;
    } else if (items.length) {
      gapAdvice = "Import at least one top and one bottom to start generating outfits.";
    }
    if (!gapAdvice && tops && bottoms && !counts.wholebody_up) gapAdvice = "No jackets or outer layers yet — one layer piece multiplies your cold-weather options.";

    const palette = [...colorCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 10);
    const maxCount = Math.max(1, ...TYPE_LABELS.map(([id]) => counts[id] || 0));

    return { counts, palette, maxCount, totalWears, mostWorn, neverWorn, costPerWear, gapAdvice, looks: outfits.length };
  }, [items, outfits]);

  if (!items.length) {
    return <p className="status empty">Import a few pieces first — insights appear once your closet has data.</p>;
  }

  return (
    <section className="insights" aria-label="Wardrobe insights">
      <div className="insight-tiles">
        <div className="insight-tile"><strong>{items.length}</strong><span>pieces</span></div>
        <div className="insight-tile"><strong>{stats.looks}</strong><span>looks styled</span></div>
        <div className="insight-tile"><strong>{stats.totalWears}</strong><span>wears logged</span></div>
        <div className="insight-tile"><strong>{stats.neverWorn.length}</strong><span>never worn</span></div>
      </div>

      {stats.gapAdvice && <p className="insight-advice">{stats.gapAdvice}</p>}

      <div className="insight-columns">
        <div className="insight-block">
          <h3>Category balance</h3>
          {TYPE_LABELS.map(([id, label]) => (
            <div className="insight-bar-row" key={id}>
              <span className="insight-bar-label">{label}</span>
              <span className="insight-bar-track"><span className="insight-bar" style={{ width: `${((stats.counts[id] || 0) / stats.maxCount) * 100}%` }} /></span>
              <span className="insight-bar-count">{stats.counts[id] || 0}</span>
            </div>
          ))}
          {stats.palette.length > 1 && (
            <>
              <h3>Color story</h3>
              <div className="insight-palette" aria-label="Wardrobe colors by share">
                {stats.palette.map(([color, count]) => (
                  <span key={color} style={{ backgroundColor: color, flexGrow: count }} title={`${color} · ${count} ${count === 1 ? "piece" : "pieces"}`} />
                ))}
              </div>
            </>
          )}
        </div>

        <div className="insight-block">
          <h3>Most worn</h3>
          {stats.mostWorn.length ? (
            <ul className="insight-list">
              {stats.mostWorn.map(({ item, wears }) => (
                <li key={item.id}>
                  <span className="insight-thumb"><OptimizedImage src={item.thumbnail || item.image} alt="" sizes="36px" breakpoints={[36, 72]} /></span>
                  <span className="insight-item-name">{item.name}</span>
                  <span className="insight-item-value">{wears}×</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="insight-empty">Hit "Wear it" on a look to start tracking what you actually wear.</p>
          )}

          <h3>Cost per wear</h3>
          {stats.costPerWear.length ? (
            <ul className="insight-list">
              {stats.costPerWear.map(({ item, wears, cpw }) => (
                <li key={item.id}>
                  <span className="insight-thumb"><OptimizedImage src={item.thumbnail || item.image} alt="" sizes="36px" breakpoints={[36, 72]} /></span>
                  <span className="insight-item-name">{item.name}</span>
                  <span className="insight-item-value">${cpw.toFixed(0)}{wears ? ` · ${wears}×` : " · unworn"}</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="insight-empty">Add prices to your pieces (in the item editor) to see cost per wear.</p>
          )}
        </div>
      </div>

      <CapsulePlanner items={items} />
    </section>
  );
}
