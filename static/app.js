/* Stylist — client. All state lives in localStorage; the server is stateless. */
(() => {
"use strict";

// ---------- storage ----------
const store = {
  get(k, fallback) {
    try { const v = localStorage.getItem(k); return v ? JSON.parse(v) : fallback; }
    catch { return fallback; }
  },
  set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch {} },
};

let profile = store.get("sty_profile", null);
let messages = store.get("sty_messages", []);   // [{role, content}]
let saved = store.get("sty_saved", []);          // [look]
let taste = store.get("sty_taste", { loved: [], less: [] });
let events = store.get("sty_events", []);        // [{occasion, date, vibe, note}]
let tryonEnabled = false;                        // set from /api/config
const CLOTHING = new Set(["top", "bottom", "dress", "outerwear"]);
let streaming = false;
let abortCtrl = null;
let feedLoading = false;
let currentLook = null; // look shown in the sheet
let chatGen = 0;        // bumped on "New chat" so an in-flight reply can't leak in

// ---------- retailer deep links ----------
const RETAILERS = {
  "India": [
    ["Myntra", q => `https://www.myntra.com/${encodeURIComponent(q.trim().replace(/\s+/g, "-"))}`],
    ["Nykaa",  q => `https://www.nykaafashion.com/search/?q=${encodeURIComponent(q)}`],
    ["Ajio",   q => `https://www.ajio.com/search/?text=${encodeURIComponent(q)}`],
    ["Amazon", q => `https://www.amazon.in/s?k=${encodeURIComponent(q)}`],
  ],
  "United States": [
    ["Amazon", q => `https://www.amazon.com/s?k=${encodeURIComponent(q)}`],
    ["ASOS",   q => `https://www.asos.com/us/search/?q=${encodeURIComponent(q)}`],
    ["Nordstrom", q => `https://www.nordstrom.com/sr?keyword=${encodeURIComponent(q)}`],
    ["Zara",   q => `https://www.zara.com/us/en/search?searchTerm=${encodeURIComponent(q)}`],
  ],
  "United Kingdom": [
    ["ASOS",   q => `https://www.asos.com/search/?q=${encodeURIComponent(q)}`],
    ["Amazon", q => `https://www.amazon.co.uk/s?k=${encodeURIComponent(q)}`],
    ["Next",   q => `https://www.next.co.uk/search?w=${encodeURIComponent(q)}`],
    ["Zara",   q => `https://www.zara.com/uk/en/search?searchTerm=${encodeURIComponent(q)}`],
  ],
  "Other / Global": [
    ["Amazon", q => `https://www.amazon.com/s?k=${encodeURIComponent(q)}`],
    ["ASOS",   q => `https://www.asos.com/search/?q=${encodeURIComponent(q)}`],
    ["H&M",    q => `https://www2.hm.com/en_us/search-results.html?q=${encodeURIComponent(q)}`],
    ["Zara",   q => `https://www.zara.com/us/en/search?searchTerm=${encodeURIComponent(q)}`],
  ],
};
const retailersFor = () => RETAILERS[(profile && profile.region) || "India"] || RETAILERS["Other / Global"];

const CAT_ICON = {
  top: "i-shirt", bottom: "i-pants", outerwear: "i-coat", footwear: "i-shoe",
  accessory: "i-glasses", dress: "i-dress", other: "i-bag",
};
const icon = (id, cls = "icon") => `<svg class="${cls}" aria-hidden="true"><use href="#${id}"/></svg>`;
const heartIcon = filled => icon("i-heart", filled ? "icon fill" : "icon");

// ---------- AI imagery (pollinations.ai — free, keyless) ----------
const HERO_STYLE = ", premium fashion editorial photography, shot on medium format film, soft directional light, rich muted tones, shallow depth of field, no text, no watermark, no logo";
const THUMB_STYLE = ", clean studio product photography, warm neutral seamless background, soft shadow, no text, no watermark";

function hashSeed(s) {
  let h = 5381;
  for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) >>> 0;
  return h % 100000;
}
const heroURL = (prompt, seed, w = 832, h = 1040) =>
  `https://image.pollinations.ai/prompt/${encodeURIComponent(prompt + HERO_STYLE)}?width=${w}&height=${h}&nologo=true&model=flux&seed=${seed}`;
const thumbURL = (prompt, seed) =>
  `https://image.pollinations.ai/prompt/${encodeURIComponent(prompt + THUMB_STYLE)}?width=384&height=456&nologo=true&model=flux&seed=${seed}`;

function paletteGradient(palette) {
  const cols = Array.isArray(palette) && palette.length >= 2
    ? palette.filter(c => /^#[0-9a-fA-F]{3,8}$/.test(String(c)))
    : [];
  if (cols.length < 2) return "linear-gradient(160deg, #3a2a1f, #1d1a16)";
  return `linear-gradient(160deg, ${cols.join(", ")})`;
}

// ---------- dom ----------
const $ = id => document.getElementById(id);
const messagesEl = $("messages"), emptyState = $("emptyState"), chipsEl = $("chips");
const inputEl = $("input"), sendBtn = $("sendBtn"), stopBtn = $("stopBtn");
const modal = $("modal"), toastEl = $("toast"), sheetEl = $("sheet");
const feedGrid = $("feedGrid");

// ---------- helpers ----------
const esc = s => String(s).replace(/[&<>"']/g, c => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[c]));

function toast(msg) {
  toastEl.textContent = msg;
  toastEl.hidden = false;
  toastEl.classList.remove("out");
  void toastEl.offsetWidth;               // restart the entrance animation
  clearTimeout(toastEl._t);
  clearTimeout(toastEl._t2);
  toastEl._t = setTimeout(() => {
    toastEl.classList.add("out");
    const finish = () => {
      if (toastEl.classList.contains("out")) {
        toastEl.hidden = true;
        toastEl.classList.remove("out");
      }
    };
    toastEl.addEventListener("animationend", finish, { once: true });
    toastEl._t2 = setTimeout(finish, 500); // safety net
  }, 2200);
}

function scrollToBottom(force) {
  const nearBottom = window.innerHeight + window.scrollY >= document.body.scrollHeight - 240;
  if (force || nearBottom) window.scrollTo({ top: document.body.scrollHeight });
}

// wire lazy image fade-in + graceful fallback for a container we just injected
function hydrateImages(root) {
  root.querySelectorAll("img[data-hydrate]").forEach(img => {
    img.removeAttribute("data-hydrate");
    const done = () => img.classList.add("loaded");
    if (img.complete && img.naturalWidth > 0) done();
    else {
      img.addEventListener("load", done, { once: true });
      img.addEventListener("error", () => img.remove(), { once: true });
    }
  });
}

// tiny markdown: bold, italic, code, bullets, paragraphs
function md(text) {
  const inline = s => esc(s)
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/(^|\W)\*([^*\n]+)\*(?=\W|$)/g, "$1<em>$2</em>")
    .replace(/`([^`\n]+)`/g, "<code>$1</code>");
  const out = [];
  let list = null;
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    const m = line.match(/^[-*•]\s+(.*)/);
    if (m) { (list ??= []).push(`<li>${inline(m[1])}</li>`); continue; }
    if (list) { out.push(`<ul>${list.join("")}</ul>`); list = null; }
    if (line) out.push(`<p>${inline(line.replace(/^#{1,4}\s*/, ""))}</p>`);
  }
  if (list) out.push(`<ul>${list.join("")}</ul>`);
  return out.join("");
}

// ---------- weather (open-meteo, free & keyless) ----------
const WMO = c =>
  c <= 1 ? "clear" : c <= 3 ? "cloudy" : c <= 48 ? "misty" :
  c <= 67 ? "rainy" : c <= 77 ? "snowy" : c <= 82 ? "showers" : "stormy";

async function getWeather() {
  const cached = store.get("sty_weather", null);
  if (cached && Date.now() - cached.at < 60 * 60 * 1000) return cached.text;
  // Hard-race the whole permission+fix flow: an unanswered permission prompt
  // fires neither callback, and the feed must never block on it.
  const pos = await Promise.race([
    new Promise(res => {
      if (!navigator.geolocation) return res(null);
      navigator.geolocation.getCurrentPosition(
        p => res(p.coords), () => res(null), { timeout: 3000, maximumAge: 600000 });
    }),
    new Promise(res => setTimeout(() => res(null), 3500)),
  ]);
  if (!pos) return null;
  try {
    const r = await fetch(
      `https://api.open-meteo.com/v1/forecast?latitude=${pos.latitude}&longitude=${pos.longitude}&current=temperature_2m,weather_code`,
      { signal: AbortSignal.timeout(4000) });
    const j = await r.json();
    const t = Math.round(j.current.temperature_2m);
    const text = `${t}°C, ${WMO(j.current.weather_code)}`;
    store.set("sty_weather", { at: Date.now(), text });
    return text;
  } catch { return null; }
}

// ---------- FEED ----------
// Local date, not UTC — the daily feed should roll over at the user's midnight.
const todayKey = () => {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
};
const profileSig = () => JSON.stringify(profile || {});

function feedSkeleton() {
  // Skeletons mirror the final card layout (kicker, title, tagline).
  feedGrid.innerHTML = Array.from({ length: 5 }, () =>
    `<div class="skeleton">
       <div class="sk-text">
         <span class="sk-line w30"></span>
         <span class="sk-line w70"></span>
         <span class="sk-line w50"></span>
       </div>
     </div>`).join("");
}

function collectionToLook(c) {
  return {
    title: c.title, occasion: c.occasion, why: c.why, tip: c.tip,
    palette: c.palette, image_prompt: c.image_prompt, items: c.items || [],
  };
}

function feedCardHTML(c) {
  const seed = hashSeed(c.title + todayKey());
  const dots = (c.palette || []).slice(0, 4)
    .filter(p => /^#[0-9a-fA-F]{3,8}$/.test(String(p)))
    .map(p => `<span class="dot" style="background:${esc(p)}"></span>`).join("");
  const first = (c.items || [])[0];
  const more = Math.max(0, (c.items || []).length - 1);
  // price leads (never truncates); the item name absorbs the ellipsis
  const chip = first
    ? `<span class="card-chip">${icon(CAT_ICON[first.category] || CAT_ICON.other, "icon sm")}${first.price ? `${esc(first.price)} · ` : ""}${esc(first.name)}${more ? ` +${more}` : ""}</span>`
    : "";
  const savedNow = isSaved(collectionToLook(c));
  // note: the card itself is an <article>, not a <button> — it hosts buttons
  return `<article class="feed-card" role="button" tabindex="0" aria-label="Open look: ${esc(c.title)}">
    <div class="card-bg" style="background:${paletteGradient(c.palette)}"></div>
    <img class="card-img" data-hydrate loading="lazy" alt="" src="${esc(heroURL(c.image_prompt || c.title, seed))}">
    <div class="card-scrim"></div>
    ${chip}
    <div class="card-actions">
      <button class="card-act${savedNow ? " on" : ""}" data-card-save aria-label="Save look">${heartIcon(savedNow)}</button>
      <button class="card-act" data-card-less aria-label="Less like this">${icon("i-less")}</button>
    </div>
    <div class="card-text">
      <p class="card-kicker">${esc(c.occasion || "")}</p>
      <h3 class="card-title">${esc(c.title)}</h3>
      <p class="card-tagline">${esc(c.tagline || "")}</p>
      <div class="card-foot">${dots}<span class="card-cta">Shop the look${icon("i-arrow-r", "icon sm")}</span></div>
    </div>
  </article>`;
}

// Append one card, consuming a skeleton slot if one is waiting.
function placeFeedCard(c) {
  const wrap = document.createElement("div");
  wrap.innerHTML = feedCardHTML(c);
  const card = wrap.firstElementChild;
  card.addEventListener("click", e => {
    const saveBtn = e.target.closest("[data-card-save]");
    const lessBtn = e.target.closest("[data-card-less]");
    if (saveBtn) {
      e.stopPropagation();
      const look = collectionToLook(c);
      if (isSaved(look)) {
        saved = saved.filter(s => lookKey(s) !== lookKey(look));
        saveBtn.classList.remove("on");
        saveBtn.innerHTML = heartIcon(false);
        toast("Removed from saved");
      } else {
        saved.push(look);
        pushTaste("loved", look);
        saveBtn.classList.add("on");
        saveBtn.innerHTML = heartIcon(true);
        toast("Saved to your looks");
      }
      store.set("sty_saved", saved);
      refreshSavedBadge();
      return;
    }
    if (lessBtn) {
      e.stopPropagation();
      pushTaste("less", collectionToLook(c));
      toast("Got it — dialing this down");
      return;
    }
    openSheet(collectionToLook(c));
  });
  card.addEventListener("keydown", e => {
    if ((e.key === "Enter" || e.key === " ") && e.target === card) {
      e.preventDefault();
      openSheet(collectionToLook(c));
    }
  });
  const sk = feedGrid.querySelector(".skeleton");
  if (sk) {
    // The tile resolves in place — only its text enters, no remove+re-enter.
    card.classList.add("from-skeleton");
    feedGrid.replaceChild(card, sk);
  } else {
    feedGrid.appendChild(card);
  }
  hydrateImages(card);
  return card;
}

// ---------- inline style quiz (progressive profiling, zero model cost) ----------
const QUIZ_BANK = [
  { id: "colours", q: "Which colours do you reach for most?", opts: ["Earth tones", "Monochrome", "Pastels", "Jewel tones"] },
  { id: "power_colour", q: "You get the most compliments wearing…", opts: ["Dark neutrals", "Reds & rust", "Blues & greens", "Whites & creams"] },
  { id: "dresses", q: "Dresses or separates?", opts: ["Dresses, always", "Separates I can mix", "Co-ord sets", "Depends on the day"] },
  { id: "footwear", q: "Your everyday footwear?", opts: ["White sneakers", "Flats & ballerinas", "Block heels", "Heels, always"] },
  { id: "jewellery", q: "Your jewellery lane?", opts: ["Gold", "Silver", "Minimal / none", "Statement pieces"] },
  { id: "ethnic", q: "Ethnic wear — how often?", opts: ["Part of my week", "Festive & weddings", "Rarely", "Show me fusion"] },
  { id: "fit", q: "How do you like your fit?", opts: ["Relaxed & flowy", "Tailored & structured", "Fitted", "Mix it up"] },
  { id: "prints", q: "Prints or plains?", opts: ["Plains mostly", "Florals", "Subtle textures", "Bold prints"] },
];
let quizAnswers = store.get("sty_quiz", {});

function maybeInsertQuiz() {
  if (feedGrid.querySelector(".quiz-card")) return;
  const next = QUIZ_BANK.find(q => !(q.id in quizAnswers));
  if (!next) return;
  const el = document.createElement("article");
  el.className = "quiz-card";
  el.innerHTML = `<p class="quiz-kicker">Style quotient</p>
    <h3 class="quiz-q">${esc(next.q)}</h3>
    <div class="quiz-options">${next.opts.map(o => `<button class="pill" data-q="${esc(o)}">${esc(o)}</button>`).join("")}</div>`;
  el.addEventListener("click", e => {
    const b = e.target.closest("[data-q]");
    if (!b) return;
    quizAnswers[next.id] = b.dataset.q;
    store.set("sty_quiz", quizAnswers);
    el.innerHTML = `<p class="quiz-kicker">Style quotient</p>
      <h3 class="quiz-q">${esc(b.dataset.q)}</h3>
      <div class="quiz-done">${icon("i-check")}<span>Noted — your next edit gets sharper.</span></div>`;
    refreshTasteChip();
  });
  feedGrid.insertBefore(el, feedGrid.children[2] || null);
}

function refreshTasteChip() {
  $("tasteChip").hidden = !(taste.loved.length || taste.less.length || Object.keys(quizAnswers).length);
}

// ---------- occasion studio (her calendar is the retention loop) ----------
const daysUntil = date =>
  Math.round((new Date(date + "T00:00") - new Date(todayKey() + "T00:00")) / 86400000);

function pruneEvents() {
  events = events.filter(e => e.date && daysUntil(e.date) >= 0).slice(-5);
  store.set("sty_events", events);
}

function eventSummaries() {
  return events.map(e =>
    `${e.occasion} on ${e.date} (${daysUntil(e.date)} days away), wants to ${e.vibe}${e.note ? ` — ${e.note}` : ""}`);
}

function renderEventChip() {
  const chip = $("eventChip");
  const next = [...events].sort((a, b) => a.date.localeCompare(b.date))[0];
  if (!next || daysUntil(next.date) > 30) { chip.hidden = true; return; }
  const d = daysUntil(next.date);
  chip.innerHTML = `${icon("i-spark", "icon sm")}${esc(next.occasion)} ${d === 0 ? "— today!" : `in ${d} day${d === 1 ? "" : "s"}`}`;
  chip.hidden = false;
  chip.onclick = () => {
    switchTab("chat");
    const msg = d === 0
      ? `My ${next.occasion} is TODAY. Final check — confirm the look and any last-minute fixes.`
      : `My ${next.occasion} is in ${d} days (${next.date}). What should be done by now, and let's finalize the look — I want to ${next.vibe}.`;
    if (streaming) { inputEl.value = msg; autosize(); }
    else send(msg);
  };
}

function readPillRow(name) {
  const row = document.querySelector(`.pill-row[data-name="${name}"]`);
  const on = [...row.querySelectorAll(".pill.on")].map(p => p.dataset.v);
  return row.classList.contains("multi") ? on.join(", ") : on[0] || "";
}

function setGreeting(text) {
  if (text) $("feedGreeting").textContent = text; // roman display voice
}

function renderFeed(data) {
  setGreeting(data.greeting);
  feedGrid.innerHTML = "";
  (data.collections || []).forEach((c, i) => {
    // Cached path renders synchronously — choreograph the entrance.
    placeFeedCard(c).style.animationDelay = `${Math.min(i * 60, 300)}ms`;
  });
  maybeInsertQuiz();
}

function rememberShown(collections) {
  const seen = store.get("sty_seen", []);
  const merged = [...seen, ...collections.map(c => ({ t: String(c.title).slice(0, 60), d: todayKey() }))];
  store.set("sty_seen", merged.slice(-24));
}
const recentTitles = () => store.get("sty_seen", []).map(s => s.t).slice(-15);

async function loadFeed(force) {
  if (feedLoading || !profile) return;
  // Snapshot the profile at REQUEST time: if it changes mid-flight, the
  // response is stored under the old signature and we regenerate after.
  const sig = profileSig();
  const prof = profile;
  const cached = store.get("sty_feed", null);
  if (!force && cached && cached.date === todayKey() && cached.sig === sig) {
    renderFeed(cached.data);
    if (cached.weather) showWeather(cached.weather);
    return;
  }

  feedLoading = true;
  $("refreshFeed").classList.add("spinning");
  feedSkeleton();

  const weather = await getWeather();
  if (weather) showWeather(weather);
  // Novelty memory: everything shown recently (across days) is excluded.
  const avoid = recentTitles();

  const data = { greeting: "", collections: [] };
  try {
    const res = await fetch("/api/feed", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        profile: prof,
        context: {
          weather: weather || "unknown", loved: taste.loved, less: taste.less,
          avoid, quiz: quizAnswers, events: eventSummaries(),
        },
      }),
    });
    if (!res.ok) {
      const j = await res.json().catch(() => ({}));
      throw new Error(j.error || `Feed failed (${res.status})`);
    }

    // The feed streams: greeting first, then one collection at a time —
    // each card renders the moment the model finishes composing it.
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    outer: while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const events = buf.split("\n\n");
      buf = events.pop();
      for (const ev of events) {
        const line = ev.split("\n").find(l => l.startsWith("data: "));
        if (!line) continue;
        let payload;
        try { payload = JSON.parse(line.slice(6)); } catch { continue; }
        if (payload.type === "greeting") {
          data.greeting = payload.text;
          setGreeting(payload.text);
        } else if (payload.type === "collection") {
          data.collections.push(payload.data);
          placeFeedCard(payload.data);
          const p = $("feedProgress");
          p.textContent = `Styling look ${Math.min(data.collections.length, 5)} of 5…`;
          p.hidden = false;
        } else if (payload.type === "error") {
          throw new Error(payload.error);
        } else if (payload.type === "done") {
          break outer;
        }
      }
    }

    if (!data.collections.length) throw new Error("Empty feed — try again.");
    feedGrid.querySelectorAll(".skeleton").forEach(s => s.remove());
    store.set("sty_feed", { date: todayKey(), sig, data, weather });
    rememberShown(data.collections);
    maybeInsertQuiz();
  } catch (err) {
    if (data.collections.length) {
      // Partial feed arrived before the failure — keep it, don't wipe it.
      feedGrid.querySelectorAll(".skeleton").forEach(s => s.remove());
      store.set("sty_feed", { date: todayKey(), sig, data, weather });
      rememberShown(data.collections);
      toast("Some looks didn't make it — refresh for more");
    } else {
      // no inline handlers — CSP (script-src 'self') forbids them
      feedGrid.innerHTML = `<div class="feed-error">
        <p>${esc(err.message || "Couldn't load your feed.")}</p>
        <button class="refresh-btn" id="feedRetry">Try again</button>
      </div>`;
      const retry = $("feedRetry");
      if (retry) retry.onclick = () => loadFeed(true);
    }
  }
  feedLoading = false;
  $("refreshFeed").classList.remove("spinning");
  $("feedProgress").hidden = true;
  if (profileSig() !== sig) loadFeed(true);  // profile changed mid-flight
}

function showWeather(text) {
  const chip = $("weatherChip");
  chip.textContent = `◦ ${text}`;
  chip.hidden = false;
}

$("refreshFeed").onclick = () => loadFeed(true);

// ---------- look sheet ----------
function openSheet(look) {
  sheetEl.classList.remove("closing"); // cancels any in-flight close
  currentLook = look;
  const seed = hashSeed(look.title + todayKey());
  const items = (look.items || []).map((item, i) => {
    const q = item.search || item.name || "";
    const tryBtn = tryonEnabled && CLOTHING.has(item.category)
      ? `<button class="try-btn" data-try="${i}">${icon("i-wand", "icon sm")}Try on</button>` : "";
    const links = tryBtn + retailersFor().map(([name, fn], j) =>
      `<a class="shop-link${j === 0 ? " primary" : ""}" href="${esc(fn(q))}" target="_blank" rel="noopener">${name}${icon("i-out", "icon sm")}</a>`
    ).join("");
    const thumb = item.image_prompt
      ? `<div class="thumb-wrap">
           <div class="thumb-bg" style="background:${paletteGradient(look.palette)}"></div>
           <img class="item-thumb" data-hydrate loading="lazy" alt="" src="${esc(thumbURL(item.image_prompt, seed + i + 1))}">
         </div>`
      : `<div class="thumb-wrap"><div class="thumb-bg" style="background:${paletteGradient(look.palette)}"></div></div>`;
    return `<div class="sheet-item">
      ${thumb}
      <div class="sheet-item-body">
        <div class="item-top">
          <span class="item-name">${esc(item.name || "")}</span>
          ${item.price ? `<span class="item-price">${esc(item.price)}</span>` : ""}
        </div>
        <div class="shop-row">${links}</div>
      </div>
    </div>`;
  }).join("");

  // "More from today's edit" — navigate between the day's looks in place.
  const feedCache = store.get("sty_feed", null);
  const others = feedCache
    ? (feedCache.data.collections || []).filter(x => x.title !== look.title).slice(0, 6)
    : [];
  const rail = others.length ? `
    <p class="sheet-section">More from today's edit</p>
    <div class="rail">${others.map((o, i) => `
      <button class="rail-card" data-rail="${i}" aria-label="Open look: ${esc(o.title)}">
        <div class="card-bg" style="position:absolute;inset:0;background:${paletteGradient(o.palette)}"></div>
        <img class="card-img" data-hydrate loading="lazy" alt="" src="${esc(heroURL(o.image_prompt || o.title, hashSeed(o.title + todayKey()), 384, 480))}">
        <div class="card-scrim"></div>
        <span class="rail-title">${esc(o.title)}</span>
      </button>`).join("")}
    </div>` : "";

  const savedNow = isSaved(look);
  $("sheetBody").innerHTML = `
    <div class="sheet-hero">
      <div class="card-bg" style="background:${paletteGradient(look.palette)}"></div>
      ${look.image_prompt ? `<img class="card-img" data-hydrate alt="" src="${esc(heroURL(look.image_prompt, seed))}">` : ""}
      <div class="card-scrim"></div>
      <div class="card-text">
        <p class="card-kicker">${esc(look.occasion || "")}</p>
        <h3 class="card-title">${esc(look.title || "Look")}</h3>
      </div>
    </div>
    <div class="sheet-content">
      ${look.why ? `<p class="sheet-why">${esc(look.why)}</p>` : ""}
      ${look.tip ? `<div class="tip-box">${icon("i-spark")}<span><b>Stylist's tip</b> — ${esc(look.tip)}</span></div>` : ""}
      <p class="sheet-section">The pieces</p>
      ${items}
      ${rail}
      <div class="taste-row">
        <button class="taste-btn" id="tasteMore">More like this</button>
        <button class="taste-btn" id="tasteLess">Less like this</button>
      </div>
      <div class="sheet-actions">
        <button class="sheet-save${savedNow ? " saved" : ""}" id="sheetSave">${heartIcon(savedNow)}<span>${savedNow ? "Saved" : "Save"}</span></button>
        <button class="sheet-save" id="sheetShare">${icon("i-share")}<span>Share</span></button>
        <button class="sheet-refine" id="sheetRefine">Refine in chat</button>
      </div>
    </div>`;
  sheetEl.hidden = false;
  document.body.style.overflow = "hidden";
  sheetEl.querySelector(".sheet-card").scrollTop = 0;
  hydrateImages($("sheetBody"));
  const closeBtn = sheetEl.querySelector(".sheet-close");
  if (closeBtn) closeBtn.focus({ preventScroll: true });
  $("sheetBody").querySelectorAll("[data-rail]").forEach(b => {
    b.onclick = () => openSheet(collectionToLook(others[+b.dataset.rail]));
  });
  $("sheetBody").querySelectorAll("[data-try]").forEach(b => {
    b.onclick = () => tryOn((look.items || [])[+b.dataset.try], look);
  });

  $("sheetSave").onclick = e => {
    const btn = e.currentTarget;
    if (isSaved(look)) {
      saved = saved.filter(s => lookKey(s) !== lookKey(look));
      btn.classList.remove("saved");
      btn.innerHTML = `${heartIcon(false)}<span>Save</span>`;
      toast("Removed from saved");
    } else {
      saved.push(look);
      btn.classList.add("saved");
      btn.innerHTML = `${heartIcon(true)}<span>Saved</span>`;
      toast("Saved to your looks");
    }
    store.set("sty_saved", saved);
    refreshSavedBadge();
  };
  $("sheetRefine").onclick = () => {
    const msg = `Refine the "${look.title}" look (${look.occasion || "no occasion"}) — suggest swaps or variations that fit my profile.`;
    closeSheet();
    switchTab("chat");
    if (streaming) {
      // send() would silently no-op mid-stream — park the text instead.
      inputEl.value = msg;
      autosize();
      toast("Finishing the current reply — tap send when ready");
    } else {
      send(msg);
    }
  };
  $("tasteMore").onclick = () => { pushTaste("loved", look); toast("Noted — more of this vibe"); };
  $("tasteLess").onclick = () => { pushTaste("less", look); toast("Got it — dialing this down"); };
  $("sheetShare").onclick = async () => {
    const shop = retailersFor()[0];
    const lines = (look.items || []).map(i =>
      `• ${i.name}${i.price ? ` (${i.price})` : ""} → ${shop[1](i.search || i.name)}`);
    const text = `${look.title} — ${look.occasion || ""}\n${look.why || ""}\n\n${lines.join("\n")}\n\nStyled by Stylist ✦`;
    if (navigator.share) {
      try { await navigator.share({ title: look.title, text }); } catch { /* user dismissed */ }
    } else if (navigator.clipboard) {
      await navigator.clipboard.writeText(text).catch(() => {});
      toast("Look copied — paste it anywhere");
    }
  };
}

function pushTaste(kind, look) {
  const tag = `${look.title} (${look.occasion || ""})`.slice(0, 60);
  taste[kind] = [...new Set([tag, ...taste[kind]])].slice(0, 8);
  store.set("sty_taste", taste);
  refreshTasteChip();
}

function closeSheet() {
  if (sheetEl.hidden || sheetEl.classList.contains("closing")) return;
  document.body.style.overflow = "";
  currentLook = null;
  sheetEl.classList.add("closing");
  const finish = () => {
    if (!sheetEl.classList.contains("closing")) return; // reopened mid-close
    sheetEl.hidden = true;
    sheetEl.classList.remove("closing");
  };
  sheetEl.querySelector(".sheet-card").addEventListener("animationend", finish, { once: true });
  setTimeout(finish, 400); // safety net — finish is idempotent
}
sheetEl.addEventListener("click", e => { if (e.target.closest("[data-close]")) closeSheet(); });
function closeModal() {
  if (modal.hidden || modal.classList.contains("closing")) return;
  modal.classList.add("closing");
  const finish = () => {
    if (!modal.classList.contains("closing")) return; // reopened mid-close
    modal.hidden = true;
    modal.classList.remove("closing");
  };
  modal.querySelector(".modal-card").addEventListener("animationend", finish, { once: true });
  setTimeout(finish, 300); // safety net — finish is idempotent
}
document.addEventListener("keydown", e => {
  if (e.key !== "Escape") return;
  if (!sheetEl.hidden) closeSheet();
  else if (!modal.hidden && profile) closeModal();
});
$("modalClose").onclick = closeModal;
modal.addEventListener("click", e => {
  if (e.target === modal && profile) closeModal();   // backdrop dismiss
});

// ---------- assistant message parsing ----------
function parseAssistant(raw) {
  const segments = [];
  const fence = /```(look|chips)\s*\n([\s\S]*?)```/g;
  let last = 0, m;
  while ((m = fence.exec(raw))) {
    const between = raw.slice(last, m.index);
    if (between.trim()) segments.push({ type: "text", text: between });
    try {
      const data = JSON.parse(m[2]);
      segments.push({ type: m[1], data });
    } catch { /* malformed block — drop it */ }
    last = m.index + m[0].length;
  }
  const tail = raw.slice(last);

  // An unclosed TAGGED fence — a look/chips block is still streaming in.
  const tagOpen = /```(look|chips)\b[\s\S]*$/.exec(tail);
  if (tagOpen && !tail.slice(tagOpen.index + 3).includes("```")) {
    const before = tail.slice(0, tagOpen.index);
    if (before.trim()) segments.push({ type: "text", text: before });
    segments.push({ type: "pending", kind: tagOpen[1], text: tail.slice(tagOpen.index) });
    return segments;
  }

  // A fence opener whose tag hasn't fully streamed yet ("`", "``", "```chi").
  // Hold the fragment back; if the message ends here it renders as text.
  const partial = /`{1,3}[a-z]{0,5}$/.exec(tail);
  if (partial) {
    const before = tail.slice(0, partial.index);
    if (before.trim()) segments.push({ type: "text", text: before });
    segments.push({ type: "pending", kind: null, text: tail.slice(partial.index) });
    return segments;
  }

  // Anything else — including complete generic ``` fences — is plain text.
  if (tail.trim()) segments.push({ type: "text", text: tail });
  return segments;
}

const lookKey = look => JSON.stringify([look.title, (look.items || []).map(i => i.name)]);
const isSaved = look => saved.some(s => lookKey(s) === lookKey(look));

function lookCardHTML(look, opts = {}) {
  const seed = hashSeed(look.title || "look");
  const hero = look.image_prompt
    ? `<div class="look-hero">
         <div class="card-bg" style="background:${paletteGradient(look.palette)}"></div>
         <img class="card-img" data-hydrate loading="lazy" alt="" src="${esc(heroURL(look.image_prompt, seed, 832, 520))}">
       </div>`
    : "";
  const items = (look.items || []).map(item => {
    const q = item.search || item.name || "";
    const links = retailersFor().map(([name, fn], i) =>
      `<a class="shop-link${i === 0 ? " primary" : ""}" href="${esc(fn(q))}" target="_blank" rel="noopener">${name}${icon("i-out", "icon sm")}</a>`
    ).join("");
    return `<div class="item">
      <div class="item-top">
        <span class="item-cat">${icon(CAT_ICON[item.category] || CAT_ICON.other, "icon sm")}</span>
        <span class="item-name">${esc(item.name || "")}</span>
        ${item.price ? `<span class="item-price">${esc(item.price)}</span>` : ""}
      </div>
      <div class="shop-row">${links}</div>
    </div>`;
  }).join("");

  const savedNow = isSaved(look);
  const saveBtn = opts.removable
    ? `<button class="save-btn saved" data-remove title="Remove" aria-label="Remove look">${icon("i-x")}</button>`
    : `<button class="save-btn${savedNow ? " saved" : ""}" data-save title="Save look" aria-label="Save look">${heartIcon(savedNow)}</button>`;

  return `<article class="look-card" data-look="${esc(JSON.stringify(look))}">
    ${hero}
    <div class="look-body">
      <div class="look-head">
        <div>
          <h3 class="look-title">${esc(look.title || "Look")}</h3>
          ${look.occasion ? `<span class="look-occasion">${esc(look.occasion)}</span>` : ""}
        </div>
        ${saveBtn}
      </div>
      ${look.why ? `<p class="look-why">${esc(look.why)}</p>` : ""}
      ${items}
    </div>
  </article>`;
}

function renderAssistantInto(el, raw, done) {
  const segments = parseAssistant(raw);
  let html = "";
  let chips = null;
  for (const seg of segments) {
    if (seg.type === "text") html += `<div class="prose">${md(seg.text)}</div>`;
    else if (seg.type === "look") html += lookCardHTML(seg.data);
    else if (seg.type === "chips") chips = seg.data;
    else if (seg.type === "pending") {
      if (done) {
        // Never silently drop content: an unresolved fence renders as text.
        if (seg.text && seg.text.trim()) html += `<div class="prose">${md(seg.text)}</div>`;
      } else if (seg.kind === "look") {
        html += `<div class="look-pending">Putting a look together…</div>`;
      }
      // chips / unknown fragments render nothing while streaming
    }
  }
  if (!done && !html) html = `<div class="typing"><i></i><i></i><i></i></div>`;
  el.innerHTML = html;
  hydrateImages(el);
  return chips;
}

// ---------- chat rendering ----------
function addUserMsg(text, imgSrc) {
  const div = document.createElement("div");
  div.className = "msg user";
  div.innerHTML = `<div class="bubble">${imgSrc ? `<img src="${esc(imgSrc)}" alt="Shared photo">` : ""}${esc(text)}</div>`;
  messagesEl.appendChild(div);
  return div;
}

// ---------- photo styling (visual search) ----------
let pendingImage = null; // { media_type, data, thumb }

async function prepareImage(file) {
  const bmp = await createImageBitmap(file);
  const scale = max => {
    const r = Math.min(1, max / Math.max(bmp.width, bmp.height));
    const cv = document.createElement("canvas");
    cv.width = Math.max(1, Math.round(bmp.width * r));
    cv.height = Math.max(1, Math.round(bmp.height * r));
    cv.getContext("2d").drawImage(bmp, 0, 0, cv.width, cv.height);
    return cv.toDataURL("image/jpeg", 0.82);
  };
  const full = scale(1024);
  const thumb = scale(220);
  return { media_type: "image/jpeg", data: full.split(",")[1], thumb };
}

function clearAttachment() {
  pendingImage = null;
  $("attachPreview").hidden = true;
}

function addSystemMsg(text) {
  const div = document.createElement("div");
  div.className = "msg system";
  div.textContent = text;
  messagesEl.appendChild(div);
}

function addAssistantShell() {
  const div = document.createElement("div");
  div.className = "msg assistant";
  messagesEl.appendChild(div);
  return div;
}

function setChips(list) {
  chipsEl.innerHTML = "";
  if (!Array.isArray(list)) return;
  for (const c of list.slice(0, 4)) {
    if (typeof c !== "string") continue;
    const b = document.createElement("button");
    b.className = "chip";
    b.textContent = c;
    b.onclick = () => send(c);
    chipsEl.appendChild(b);
  }
}

function renderHistory() {
  messagesEl.innerHTML = "";
  let lastChips = null;
  for (const m of messages) {
    if (m.role === "user") addUserMsg(m.content, m.img);
    else lastChips = renderAssistantInto(addAssistantShell(), m.content, true);
  }
  emptyState.style.display = messages.length ? "none" : "";
  setChips(messages.length ? lastChips : null);
}

// ---------- streaming chat ----------
async function send(text) {
  text = (text || "").trim();
  const img = pendingImage;
  if ((!text && !img) || streaming) return;
  if (!text) text = "Style this — find me the pieces.";
  clearAttachment();

  const gen = chatGen;
  messages.push({ role: "user", content: text, ...(img ? { img: img.thumb } : {}) });
  emptyState.style.display = "none";
  const userDiv = addUserMsg(text, img && img.thumb);
  setChips(null);
  inputEl.value = "";
  autosize();
  scrollToBottom(true);

  const shell = addAssistantShell();
  renderAssistantInto(shell, "", false);
  streaming = true;
  sendBtn.hidden = true;
  stopBtn.hidden = false;
  abortCtrl = new AbortController();

  let raw = "";
  let errored = false;

  try {
    const res = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        // history travels as text; the photo rides only with this turn
        messages: messages.map(m => ({ role: m.role, content: m.content })),
        profile: profile || {},
        ...(img ? { image: { media_type: img.media_type, data: img.data } } : {}),
      }),
      signal: abortCtrl.signal,
    });

    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      throw new Error(data.error || `Request failed (${res.status})`);
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const events = buf.split("\n\n");
      buf = events.pop();
      for (const ev of events) {
        const line = ev.split("\n").find(l => l.startsWith("data: "));
        if (!line) continue;
        let payload;
        try { payload = JSON.parse(line.slice(6)); } catch { continue; }
        if (payload.type === "text") {
          raw += payload.text;
          renderAssistantInto(shell, raw, false);
          scrollToBottom();
        } else if (payload.type === "error") {
          throw new Error(payload.error);
        }
      }
    }
  } catch (err) {
    if (err.name !== "AbortError") {
      errored = true;
      addSystemMsg(err.message || "Something went wrong — try again.");
    }
  }

  streaming = false;
  sendBtn.hidden = false;
  stopBtn.hidden = true;
  abortCtrl = null;

  // "New chat" was pressed while this reply streamed — the conversation was
  // reset, so discard everything from this turn instead of leaking it in.
  if (gen !== chatGen) return;

  if (raw.trim()) {
    messages.push({ role: "assistant", content: raw });
    const chips = renderAssistantInto(shell, raw, true);
    setChips(chips);
  } else {
    // Failed turn: unwind BOTH history and the DOM so they can't diverge,
    // and hand the text back to the composer for an easy retry.
    shell.remove();
    userDiv.remove();
    messages.pop();
    if (!errored) addSystemMsg("No reply received — try again.");
    inputEl.value = text;
    autosize();
    if (!messages.length) emptyState.style.display = "";
  }
  store.set("sty_messages", messages.slice(-40));
  scrollToBottom();
}

// ---------- saved looks ----------
function refreshSavedBadge() {
  const badge = $("savedCount");
  badge.textContent = saved.length;
  badge.hidden = saved.length === 0;
}

function renderSaved() {
  const list = $("savedList");
  list.innerHTML = saved.map(l => lookCardHTML(l, { removable: true })).join("");
  hydrateImages(list);
  $("savedEmpty").style.display = saved.length ? "none" : "";
  refreshSavedBadge();
}

document.addEventListener("click", e => {
  const saveBtn = e.target.closest("[data-save]");
  const removeBtn = e.target.closest("[data-remove]");
  if (!saveBtn && !removeBtn) return;
  const card = e.target.closest(".look-card");
  let look;
  try { look = JSON.parse(card.dataset.look); } catch { return; }

  if (saveBtn) {
    if (isSaved(look)) {
      saved = saved.filter(s => lookKey(s) !== lookKey(look));
      saveBtn.classList.remove("saved");
      saveBtn.innerHTML = heartIcon(false);
      toast("Removed from saved");
    } else {
      saved.push(look);
      saveBtn.classList.add("saved");
      saveBtn.innerHTML = heartIcon(true);
      toast("Saved to your looks");
    }
    store.set("sty_saved", saved);
    refreshSavedBadge();
  } else {
    saved = saved.filter(s => lookKey(s) !== lookKey(look));
    store.set("sty_saved", saved);
    card.remove();
    toast("Removed");
    $("savedEmpty").style.display = saved.length ? "none" : "";
    refreshSavedBadge();
  }
});

// ---------- tabs ----------
function switchTab(which) {
  document.querySelectorAll(".tab").forEach(t => t.classList.toggle("active", t.dataset.tab === which));
  $("feedPanel").classList.toggle("active", which === "feed");
  $("chatPanel").classList.toggle("active", which === "chat");
  $("savedPanel").classList.toggle("active", which === "saved");
  if (which === "saved") renderSaved();
  if (which === "feed") loadFeed(false);
}
document.querySelectorAll(".tab").forEach(tab => (tab.onclick = () => switchTab(tab.dataset.tab)));

// ---------- composer ----------
function autosize() {
  inputEl.style.height = "auto";
  inputEl.style.height = Math.min(inputEl.scrollHeight, 130) + "px";
}
inputEl.addEventListener("input", autosize);
inputEl.addEventListener("keydown", e => {
  if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(inputEl.value); }
});
$("composer").addEventListener("submit", e => { e.preventDefault(); send(inputEl.value); });
stopBtn.onclick = () => abortCtrl && abortCtrl.abort();

const fileInput = $("fileInput");
$("attachBtn").onclick = () => fileInput.click();
fileInput.onchange = async () => {
  const f = fileInput.files[0];
  fileInput.value = "";
  if (!f || !f.type.startsWith("image/")) return;
  try {
    pendingImage = await prepareImage(f);
  } catch {
    toast("Couldn't read that photo — try another");
    return;
  }
  $("attachThumb").src = pendingImage.thumb;
  $("attachPreview").hidden = false;
  inputEl.placeholder = "e.g. Find me these pieces / How do I style this?";
  inputEl.focus();
};
$("attachRemove").onclick = () => {
  clearAttachment();
  inputEl.placeholder = "Ask your stylist anything…";
};

document.querySelectorAll(".starter").forEach(b => (b.onclick = () => {
  if (b.dataset.action === "attach") fileInput.click();   // "find something similar" → photo styling
  else send(b.dataset.q);
}));

// ---------- fresh drops from favourite brands (always-live retailer links) ----------
const NEW_IN = {
  "India": b => `https://www.myntra.com/${encodeURIComponent(b.toLowerCase().replace(/\s+/g, "-"))}?sort=new`,
  "United States": b => `https://www.nordstrom.com/sr?keyword=${encodeURIComponent(b + " new arrivals")}`,
  "United Kingdom": b => `https://www.asos.com/search/?q=${encodeURIComponent(b + " new in")}`,
  "Other / Global": b => `https://www.asos.com/search/?q=${encodeURIComponent(b + " new in")}`,
};

function renderDrops() {
  const rail = $("dropsRail");
  const brands = (profile && profile.brands ? profile.brands : "")
    .split(",").map(s => s.trim()).filter(s => s.length > 1).slice(0, 6);
  if (!brands.length) { rail.hidden = true; return; }
  const linker = NEW_IN[(profile && profile.region) || "India"] || NEW_IN["Other / Global"];
  $("dropsChips").innerHTML = brands.map(b =>
    `<a class="chip" href="${esc(linker(b))}" target="_blank" rel="noopener">${esc(b)}${icon("i-out", "icon sm")}</a>`
  ).join("");
  rail.hidden = false;
}

// ---------- profile modal ----------
function openModal() {
  modal.classList.remove("closing"); // cancels any in-flight close
  modal.hidden = false;
  // First run must complete the quiz; returning users can dismiss.
  $("modalClose").hidden = !profile;
  refreshSelfieUI();
  // Smart defaults on first run — one tap to a working feed.
  const defaults = profile ? null
    : { gender: "womenswear", region: "India", budget: "mid-range, quality basics", dresscode: "business casual" };
  document.querySelectorAll(".pill-row").forEach(row => {
    const name = row.dataset.name;
    const current = profile ? profile[name] : (defaults && defaults[name]) || null;
    row.querySelectorAll(".pill").forEach(p => {
      const on = current
        ? (row.classList.contains("multi") ? current.includes(p.dataset.v) : current === p.dataset.v)
        : false;
      p.classList.toggle("on", on);
    });
  });
  $("nameInput").value = (profile && profile.name) || "";
  $("brandsInput").value = (profile && profile.brands) || "";
  $("notesInput").value = (profile && profile.notes) || "";
}

document.querySelectorAll(".pill-row").forEach(row => {
  row.addEventListener("click", e => {
    const pill = e.target.closest(".pill");
    if (!pill) return;
    if (row.classList.contains("multi")) pill.classList.toggle("on");
    else row.querySelectorAll(".pill").forEach(p => p.classList.toggle("on", p === pill));
  });
});

$("profileForm").addEventListener("submit", e => {
  e.preventDefault();
  const read = name => {
    const row = document.querySelector(`.pill-row[data-name="${name}"]`);
    const on = [...row.querySelectorAll(".pill.on")].map(p => p.dataset.v);
    return row.classList.contains("multi") ? on.join(", ") : on[0] || "";
  };
  profile = {
    name: $("nameInput").value.trim(),
    gender: read("gender") || "womenswear",
    age: read("age"),
    region: read("region") || "India",
    dresscode: read("dresscode") || "business casual",
    budget: read("budget") || "mid-range, quality basics",
    style: read("style") || "open to suggestions",
    brands: $("brandsInput").value.trim(),
    notes: $("notesInput").value.trim(),
  };
  store.set("sty_profile", profile);
  closeModal();
  toast("Profile saved — styling everything to you");
  // Re-rendering mid-stream would detach the live reply's DOM node —
  // skip it; region-link refresh happens on the next natural render.
  if (!streaming) renderHistory();
  renderDrops();
  loadFeed(true);           // profile changed → fresh feed
});

// ---------- fitting room (virtual try-on) ----------
const tryonModal = $("tryonModal");

function closeTryon() { tryonModal.hidden = true; }
$("tryonClose").onclick = closeTryon;
tryonModal.addEventListener("click", e => { if (e.target === tryonModal) closeTryon(); });

async function tryOn(item, look) {
  if (!tryonEnabled || !item) return;
  const selfie = store.get("sty_selfie", null);
  if (!selfie) {
    closeSheet();
    openModal();
    toast("Add your photo first — it stays on this device");
    return;
  }

  closeSheet();
  tryonModal.hidden = false;
  $("tryonTitle").innerHTML = `Draping it <em>on you…</em>`;
  $("tryonActions").hidden = true;
  $("tryonRemaining").hidden = true;
  $("tryonBody").innerHTML = `<div class="tryon-stage">
    <div class="tryon-wait">${esc(item.name)}<br>Usually 15–30 seconds — worth it.</div>
  </div>`;

  const garment_url = thumbURL(
    item.image_prompt || `studio product photograph of ${item.name}`,
    hashSeed(item.name || "piece"));

  try {
    const res = await fetch("/api/tryon", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        person: { media_type: selfie.media_type, data: selfie.data },
        garment_url,
      }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "Try-on failed.");

    $("tryonTitle").innerHTML = `${esc(item.name)} — <em>on you.</em>`;
    $("tryonBody").innerHTML = `<div class="tryon-stage">
      <img src="${esc(data.image_url)}" alt="You wearing ${esc(item.name)}"></div>`;
    $("tryonDownload").href = data.image_url;
    $("tryonShop").onclick = () => {
      const [, fn] = retailersFor()[0];
      window.open(fn(item.search || item.name), "_blank", "noopener");
    };
    $("tryonActions").hidden = false;
    if (typeof data.remaining === "number") {
      $("tryonRemaining").textContent = `${data.remaining} try-on${data.remaining === 1 ? "" : "s"} left today · AI render — drape and fit are indicative`;
      $("tryonRemaining").hidden = false;
    }
  } catch (err) {
    $("tryonTitle").innerHTML = `The fitting room <em>hiccuped</em>`;
    $("tryonBody").innerHTML = `<p class="tryon-err">${esc(err.message || "Try again in a minute.")}</p>`;
  }
}

// selfie management (on-device only)
function refreshSelfieUI() {
  const s = store.get("sty_selfie", null);
  $("selfieField").hidden = !tryonEnabled;
  $("selfieThumb").hidden = !s;
  if (s) $("selfieThumb").src = s.thumb;
  $("selfieAdd").textContent = s ? "Replace photo" : "Add my photo";
  $("selfieRemove").hidden = !s;
}
$("selfieAdd").onclick = () => $("selfieInput").click();
$("selfieInput").onchange = async () => {
  const f = $("selfieInput").files[0];
  $("selfieInput").value = "";
  if (!f || !f.type.startsWith("image/")) return;
  try {
    const img = await prepareImage(f);
    store.set("sty_selfie", img);
    refreshSelfieUI();
    toast("Photo saved on this device — fitting room open");
  } catch { toast("Couldn't read that photo — try another"); }
};
$("selfieRemove").onclick = () => {
  localStorage.removeItem("sty_selfie");
  refreshSelfieUI();
  toast("Photo removed");
};

// ---------- occasion studio wiring ----------
const eventModal = $("eventModal");
$("eventBtn").onclick = () => { eventModal.hidden = false; };
$("eventClose").onclick = () => { eventModal.hidden = true; };
eventModal.addEventListener("click", e => { if (e.target === eventModal) eventModal.hidden = true; });

$("eventForm").addEventListener("submit", e => {
  e.preventDefault();
  const occasion = readPillRow("evOccasion") || "special event";
  const date = $("evDate").value;
  const vibe = readPillRow("evVibe") || "stand out";
  const note = $("evNote").value.trim();

  if (date && daysUntil(date) >= 0) {
    events = [...events.filter(ev => !(ev.occasion === occasion && ev.date === date)),
              { occasion, date, vibe, note }].slice(-5);
    store.set("sty_events", events);
    renderEventChip();
  }
  eventModal.hidden = true;

  const when = date
    ? ` on ${date} — ${daysUntil(date)} days away`
    : " coming up";
  const msg = `I have a ${occasion}${when}. I want to ${vibe}.${note ? ` Venue/theme: ${note}.` : ""} ` +
    `Build my full look — outfit, jewellery, footwear — and a quick prep timeline if anything needs tailoring.`;
  switchTab("chat");
  if (streaming) { inputEl.value = msg; autosize(); toast("Finishing the current reply — tap send"); }
  else send(msg);
});

$("profileBtn").onclick = openModal;
$("wipeBtn").onclick = () => {
  if (!confirm("Clear everything Stylist knows about you on this device? This removes your profile, taste, saved looks and chat history.")) return;
  localStorage.clear();
  location.reload();
};
$("newChatBtn").onclick = () => {
  chatGen++;                                // invalidate any in-flight reply
  if (streaming && abortCtrl) abortCtrl.abort();
  messages = [];
  store.set("sty_messages", []);
  renderHistory();
  switchTab("chat");
};

// ---------- voice input (Web Speech API — Chrome/Android; hidden elsewhere) ----------
(() => {
  const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
  const micBtn = $("micBtn");
  if (!SR || !micBtn) return;
  micBtn.hidden = false;
  let rec = null;
  micBtn.onclick = () => {
    if (rec) { rec.stop(); return; }
    rec = new SR();
    rec.lang = ({ "India": "en-IN", "United States": "en-US", "United Kingdom": "en-GB" })[(profile || {}).region] || "en-IN";
    rec.interimResults = true;
    const base = inputEl.value.trim();
    rec.onresult = e => {
      let t = "";
      for (const r of e.results) t += r[0].transcript;
      inputEl.value = base ? `${base} ${t}` : t;
      autosize();
    };
    const stop = () => { micBtn.classList.remove("rec"); rec = null; };
    rec.onend = stop;
    rec.onerror = stop;
    micBtn.classList.add("rec");
    try { rec.start(); } catch { stop(); }
  };
})();

// ---------- boot ----------
renderHistory();
refreshSavedBadge();
refreshTasteChip();
renderDrops();
pruneEvents();
renderEventChip();
if (!profile) openModal();
else loadFeed(false);

if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register("/sw.js").catch(() => {});
}

// feature flags from the server (try-on appears only when configured)
fetch("/api/config").then(r => r.json()).then(cfg => {
  tryonEnabled = !!cfg.tryon;
  refreshSelfieUI();
}).catch(() => {});
})();
