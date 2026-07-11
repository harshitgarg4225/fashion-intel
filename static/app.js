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
let streaming = false;
let abortCtrl = null;
let feedLoading = false;
let currentLook = null; // look shown in the sheet
let chatGen = 0;        // bumped on "New chat" so an in-flight reply can't leak in

// ---------- retailer deep links ----------
const RETAILERS = {
  "India": [
    ["Myntra", q => `https://www.myntra.com/${encodeURIComponent(q.trim().replace(/\s+/g, "-"))}`],
    ["Amazon", q => `https://www.amazon.in/s?k=${encodeURIComponent(q)}`],
    ["Ajio",   q => `https://www.ajio.com/search/?text=${encodeURIComponent(q)}`],
    ["Flipkart", q => `https://www.flipkart.com/search?q=${encodeURIComponent(q)}`],
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
  top: "👕", bottom: "👖", outerwear: "🧥", footwear: "👟",
  accessory: "🕶️", dress: "👗", other: "🛍️",
};

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
  clearTimeout(toastEl._t);
  toastEl._t = setTimeout(() => (toastEl.hidden = true), 2200);
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
  feedGrid.innerHTML = Array.from({ length: 5 },
    (_, i) => `<div class="skeleton" style="animation-delay:${i * .06}s"></div>`).join("");
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
  return `<button class="feed-card" aria-label="Open look: ${esc(c.title)}">
    <div class="card-bg" style="background:${paletteGradient(c.palette)}"></div>
    <img class="card-img" data-hydrate loading="lazy" alt="" src="${esc(heroURL(c.image_prompt || c.title, seed))}">
    <div class="card-scrim"></div>
    <div class="card-text">
      <p class="card-kicker">${esc(c.occasion || "")}</p>
      <h3 class="card-title">${esc(c.title)}</h3>
      <p class="card-tagline">${esc(c.tagline || "")}</p>
      <div class="card-foot">${dots}<span class="card-cta">Shop the look →</span></div>
    </div>
  </button>`;
}

// Append one card, consuming a skeleton slot if one is waiting.
function placeFeedCard(c) {
  const wrap = document.createElement("div");
  wrap.innerHTML = feedCardHTML(c);
  const card = wrap.firstElementChild;
  card.onclick = () => openSheet(collectionToLook(c));
  const sk = feedGrid.querySelector(".skeleton");
  if (sk) feedGrid.replaceChild(card, sk);
  else feedGrid.appendChild(card);
  hydrateImages(card);
}

function setGreeting(text) {
  if (text) $("feedGreeting").innerHTML = `<em>${esc(text)}</em>`;
}

function renderFeed(data) {
  setGreeting(data.greeting);
  feedGrid.innerHTML = "";
  (data.collections || []).forEach(placeFeedCard);
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
        context: { weather: weather || "unknown", loved: taste.loved, less: taste.less, avoid },
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
  } catch (err) {
    if (data.collections.length) {
      // Partial feed arrived before the failure — keep it, don't wipe it.
      feedGrid.querySelectorAll(".skeleton").forEach(s => s.remove());
      store.set("sty_feed", { date: todayKey(), sig, data, weather });
      rememberShown(data.collections);
      toast("Some looks didn't make it — refresh for more");
    } else {
      feedGrid.innerHTML = `<div class="feed-error">
        <p>${esc(err.message || "Couldn't load your feed.")}</p>
        <button class="refresh-btn" onclick="document.getElementById('refreshFeed').click()">Try again</button>
      </div>`;
    }
  }
  feedLoading = false;
  $("refreshFeed").classList.remove("spinning");
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
  currentLook = look;
  const seed = hashSeed(look.title + todayKey());
  const items = (look.items || []).map((item, i) => {
    const q = item.search || item.name || "";
    const links = retailersFor().map(([name, fn], j) =>
      `<a class="shop-link${j === 0 ? " primary" : ""}" href="${esc(fn(q))}" target="_blank" rel="noopener">${name} ↗</a>`
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
      ${look.tip ? `<div class="tip-box"><span>✦</span><span><b>Stylist's tip</b> — ${esc(look.tip)}</span></div>` : ""}
      <p class="sheet-section">The pieces</p>
      ${items}
      <div class="taste-row">
        <button class="taste-btn" id="tasteMore">More like this</button>
        <button class="taste-btn" id="tasteLess">Less like this</button>
      </div>
      <div class="sheet-actions">
        <button class="sheet-save${savedNow ? " saved" : ""}" id="sheetSave">${savedNow ? "♥ Saved" : "♡ Save"}</button>
        <button class="sheet-save" id="sheetShare">↗ Share</button>
        <button class="sheet-refine" id="sheetRefine">Refine in chat</button>
      </div>
    </div>`;
  sheetEl.hidden = false;
  document.body.style.overflow = "hidden";
  hydrateImages($("sheetBody"));
  const closeBtn = sheetEl.querySelector(".sheet-close");
  if (closeBtn) closeBtn.focus({ preventScroll: true });

  $("sheetSave").onclick = e => {
    const btn = e.currentTarget;
    if (isSaved(look)) {
      saved = saved.filter(s => lookKey(s) !== lookKey(look));
      btn.classList.remove("saved"); btn.textContent = "♡ Save look";
      toast("Removed from saved");
    } else {
      saved.push(look);
      btn.classList.add("saved"); btn.textContent = "♥ Saved";
      toast("Saved ♥");
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
}

function closeSheet() {
  sheetEl.hidden = true;
  document.body.style.overflow = "";
  currentLook = null;
}
sheetEl.addEventListener("click", e => { if (e.target.closest("[data-close]")) closeSheet(); });
document.addEventListener("keydown", e => {
  if (e.key !== "Escape") return;
  if (!sheetEl.hidden) closeSheet();
  else if (!modal.hidden && profile) modal.hidden = true;
});
$("modalClose").onclick = () => { modal.hidden = true; };
modal.addEventListener("click", e => {
  if (e.target === modal && profile) modal.hidden = true;   // backdrop dismiss
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
      `<a class="shop-link${i === 0 ? " primary" : ""}" href="${esc(fn(q))}" target="_blank" rel="noopener">${name} ↗</a>`
    ).join("");
    return `<div class="item">
      <div class="item-top">
        <span class="item-cat">${CAT_ICON[item.category] || CAT_ICON.other}</span>
        <span class="item-name">${esc(item.name || "")}</span>
        ${item.price ? `<span class="item-price">${esc(item.price)}</span>` : ""}
      </div>
      <div class="shop-row">${links}</div>
    </div>`;
  }).join("");

  const savedNow = isSaved(look);
  const saveBtn = opts.removable
    ? `<button class="save-btn saved" data-remove title="Remove">✕</button>`
    : `<button class="save-btn${savedNow ? " saved" : ""}" data-save title="Save look">${savedNow ? "♥" : "♡"}</button>`;

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
function addUserMsg(text) {
  const div = document.createElement("div");
  div.className = "msg user";
  div.innerHTML = `<div class="bubble">${esc(text)}</div>`;
  messagesEl.appendChild(div);
  return div;
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
    if (m.role === "user") addUserMsg(m.content);
    else lastChips = renderAssistantInto(addAssistantShell(), m.content, true);
  }
  emptyState.style.display = messages.length ? "none" : "";
  setChips(messages.length ? lastChips : null);
}

// ---------- streaming chat ----------
async function send(text) {
  text = (text || "").trim();
  if (!text || streaming) return;

  const gen = chatGen;
  messages.push({ role: "user", content: text });
  emptyState.style.display = "none";
  const userDiv = addUserMsg(text);
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
      body: JSON.stringify({ messages, profile: profile || {} }),
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
      saveBtn.textContent = "♡";
      toast("Removed from saved");
    } else {
      saved.push(look);
      saveBtn.classList.add("saved");
      saveBtn.textContent = "♥";
      toast("Saved ♥");
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

document.querySelectorAll(".starter").forEach(b => (b.onclick = () => send(b.dataset.q)));

// ---------- profile modal ----------
function openModal() {
  modal.hidden = false;
  // First run must complete the quiz; returning users can dismiss.
  $("modalClose").hidden = !profile;
  // Smart defaults on first run — one tap to a working feed.
  const defaults = profile ? null : { region: "India", budget: "mid-range, quality basics" };
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
    gender: read("gender") || "no preference",
    region: read("region") || "India",
    budget: read("budget") || "mid-range, quality basics",
    style: read("style") || "open to suggestions",
    notes: $("notesInput").value.trim(),
  };
  store.set("sty_profile", profile);
  modal.hidden = true;
  toast("Profile saved — styling everything to you");
  // Re-rendering mid-stream would detach the live reply's DOM node —
  // skip it; region-link refresh happens on the next natural render.
  if (!streaming) renderHistory();
  loadFeed(true);           // profile changed → fresh feed
});

$("profileBtn").onclick = openModal;
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
if (!profile) openModal();
else loadFeed(false);

if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register("/sw.js").catch(() => {});
}
})();
