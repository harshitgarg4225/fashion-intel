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
let streaming = false;
let abortCtrl = null;

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

// ---------- dom ----------
const $ = id => document.getElementById(id);
const messagesEl = $("messages"), emptyState = $("emptyState"), chipsEl = $("chips");
const inputEl = $("input"), sendBtn = $("sendBtn"), stopBtn = $("stopBtn");
const modal = $("modal"), toastEl = $("toast");

// ---------- helpers ----------
const esc = s => s.replace(/[&<>"']/g, c => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[c]));

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

// ---------- assistant message parsing ----------
// Splits raw model output into segments: {type:"text"|"look"|"chips"|"pending"}
function parseAssistant(raw) {
  const segments = [];
  const fence = /```(look|chips)\s*\n([\s\S]*?)```/g;
  let last = 0, m;
  while ((m = fence.exec(raw))) {
    if (m.index > last) segments.push({ type: "text", text: raw.slice(last, m.index) });
    try {
      const data = JSON.parse(m[2]);
      segments.push({ type: m[1], data });
    } catch { /* malformed block — drop it */ }
    last = m.index + m[0].length;
  }
  const tail = raw.slice(last);
  const open = tail.match(/```(look|chips)?[^`]*$/);
  if (open) {
    const before = tail.slice(0, open.index);
    if (before.trim()) segments.push({ type: "text", text: before });
    segments.push({ type: "pending", kind: open[1] || "look" });
  } else if (tail.trim()) {
    segments.push({ type: "text", text: tail });
  }
  return segments;
}

const lookKey = look => JSON.stringify([look.title, (look.items || []).map(i => i.name)]);
const isSaved = look => saved.some(s => lookKey(s) === lookKey(look));

function lookCardHTML(look, opts = {}) {
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
    <div class="look-head">
      <div>
        <h3 class="look-title">${esc(look.title || "Look")}</h3>
        ${look.occasion ? `<span class="look-occasion">${esc(look.occasion)}</span>` : ""}
      </div>
      ${saveBtn}
    </div>
    ${look.why ? `<p class="look-why">${esc(look.why)}</p>` : ""}
    ${items}
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
    else if (seg.type === "pending" && !done)
      html += seg.kind === "chips" ? "" : `<div class="look-pending">Putting a look together…</div>`;
  }
  if (!done && !html) html = `<div class="typing"><i></i><i></i><i></i></div>`;
  el.innerHTML = html;
  return chips;
}

// ---------- chat rendering ----------
function addUserMsg(text) {
  const div = document.createElement("div");
  div.className = "msg user";
  div.innerHTML = `<div class="bubble">${esc(text)}</div>`;
  messagesEl.appendChild(div);
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

// ---------- streaming ----------
async function send(text) {
  text = (text || "").trim();
  if (!text || streaming) return;

  messages.push({ role: "user", content: text });
  emptyState.style.display = "none";
  addUserMsg(text);
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

  if (raw.trim()) {
    messages.push({ role: "assistant", content: raw });
    const chips = renderAssistantInto(shell, raw, true);
    setChips(chips);
  } else {
    shell.remove();
    if (!errored) addSystemMsg("No reply received — try again.");
    messages.pop(); // drop the unanswered user turn so history stays valid
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
  } else {
    saved = saved.filter(s => lookKey(s) !== lookKey(look));
    card.remove();
    toast("Removed");
    $("savedEmpty").style.display = saved.length ? "none" : "";
  }
  store.set("sty_saved", saved);
  refreshSavedBadge();
});

// ---------- tabs ----------
document.querySelectorAll(".tab").forEach(tab => {
  tab.onclick = () => {
    document.querySelectorAll(".tab").forEach(t => t.classList.toggle("active", t === tab));
    const which = tab.dataset.tab;
    $("chatPanel").classList.toggle("active", which === "chat");
    $("savedPanel").classList.toggle("active", which === "saved");
    if (which === "saved") renderSaved();
  };
});

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

$("newChatBtn").onclick = () => {
  if (streaming) abortCtrl && abortCtrl.abort();
  messages = [];
  store.set("sty_messages", []);
  renderHistory();
};

// ---------- profile modal ----------
function openModal() {
  modal.hidden = false;
  // hydrate current selections
  document.querySelectorAll(".pill-row").forEach(row => {
    const name = row.dataset.name;
    const current = profile ? profile[name] : null;
    row.querySelectorAll(".pill").forEach(p => {
      const on = current
        ? (row.classList.contains("multi") ? current.includes(p.dataset.v) : current === p.dataset.v)
        : false;
      p.classList.toggle("on", on);
    });
  });
  $("notesInput").value = (profile && profile.notes) || "";
}

document.querySelectorAll(".pill-row").forEach(row => {
  row.addEventListener("click", e => {
    const pill = e.target.closest(".pill");
    if (!pill) return;
    if (row.classList.contains("multi")) pill.classList.toggle("on");
    else {
      row.querySelectorAll(".pill").forEach(p => p.classList.toggle("on", p === pill));
    }
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
    gender: read("gender") || "no preference",
    region: read("region") || "India",
    budget: read("budget") || "mid-range, quality basics",
    style: read("style") || "open to suggestions",
    notes: $("notesInput").value.trim(),
  };
  store.set("sty_profile", profile);
  modal.hidden = true;
  toast("Profile saved — looks will be tailored to you");
  renderHistory(); // re-render so shop links use the new region
});

$("profileBtn").onclick = openModal;

// ---------- boot ----------
renderHistory();
refreshSavedBadge();
if (!profile) openModal();
scrollToBottom(true);
})();
