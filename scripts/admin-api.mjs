import { computeMetrics, readEvents } from "./metrics.mjs";
import { currentUser, isMultiTenant } from "./tenant.mjs";

// Operator-only funnel metrics. JSON at /api/admin/metrics (behind the auth
// gates like every /api route), HTML dashboard at /admin/metrics whose inline
// script fetches the JSON — so the page itself never leaks data.
// In multi-tenant mode access additionally requires the signed-in user's
// email to be listed in MIRA_ADMIN_EMAILS; in single-tenant mode the
// passphrase holder IS the operator.
function json(res, status, value) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(JSON.stringify(value));
}

const PAGE = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Mira — Operator Metrics</title><style>
body{margin:0;background:#ffffff;color:#111;font-family:Helvetica,Arial,sans-serif}
.wrap{max-width:880px;margin:0 auto;padding:48px 24px 96px}
.eyebrow{font-size:11px;letter-spacing:.3em;text-transform:uppercase;margin:0 0 10px;text-align:center}
h1{font-weight:600;font-size:22px;letter-spacing:.24em;text-transform:uppercase;margin:0;text-align:center}
.rule{width:56px;height:1px;background:#000;margin:20px auto 36px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:1px;background:#e5e5e5;border:1px solid #e5e5e5}
.tile{background:#fff;padding:18px 16px}
.tile b{display:block;font-size:26px;font-weight:600}
.tile span{font-size:10.5px;letter-spacing:.18em;text-transform:uppercase;color:#6e6e6e}
h2{font-size:12px;letter-spacing:.2em;text-transform:uppercase;margin:40px 0 12px}
table{border-collapse:collapse;width:100%}
td,th{border:1px solid #e5e5e5;padding:8px 12px;font-size:13px;text-align:right}
th:first-child,td:first-child{text-align:left}
#err{color:#a00;text-align:center}
</style></head><body><div class="wrap">
<p class="eyebrow">Mira</p><h1>Operator Metrics</h1><div class="rule"></div>
<p id="err"></p><div id="tiles" class="tiles"></div>
<h2>Last 14 days</h2><table id="days"><thead><tr><th>Day</th><th>Signups</th><th>Active</th><th>Renders</th><th>Journal logs</th><th>Share views</th></tr></thead><tbody></tbody></table>
<script>
const pct = (value) => value == null ? "—" : (value * 100).toFixed(0) + "%";
fetch("/api/admin/metrics", { cache: "no-store" }).then(async (response) => {
  const data = await response.json();
  if (!response.ok) { document.getElementById("err").textContent = data.error || "Not authorized."; return; }
  const tiles = [
    ["Signups", data.totals.signups],
    ["Activation (1st render)", pct(data.activation.rate)],
    ["Journalers", pct(data.journalers.rate)],
    ["D1 retention", pct(data.retention.d1.rate)],
    ["D7 retention", pct(data.retention.d7.rate)],
    ["Renders", data.totals.renders],
    ["Journal logs", data.totals.journalLogs],
    ["Shares created", data.totals.sharesCreated],
    ["Share views", data.totals.shareViews],
    ["Referred signups", data.totals.referredSignups + " (" + pct(data.viral.referredShare) + ")"],
    ["View → signup", pct(data.viral.viewToSignup)],
    ["Referral activations", data.totals.referralActivations],
  ];
  document.getElementById("tiles").innerHTML = tiles.map(([label, value]) => '<div class="tile"><b>' + value + "</b><span>" + label + "</span></div>").join("");
  document.querySelector("#days tbody").innerHTML = data.days.map((day) =>
    "<tr><td>" + day.date + "</td><td>" + day.signups + "</td><td>" + day.activeUsers + "</td><td>" + day.renders + "</td><td>" + day.journalLogs + "</td><td>" + day.shareViews + "</td></tr>").join("");
});
</script></div></body></html>`;

export function adminApi(options = {}) {
  const setting = (name, fallback = "") => options.env?.[name] || process.env[name] || fallback;

  function isOperator() {
    if (!isMultiTenant(options.env)) return true; // passphrase holder = operator
    const allowed = setting("MIRA_ADMIN_EMAILS").split(",").map((email) => email.trim().toLowerCase()).filter(Boolean);
    const email = (currentUser()?.email || "").toLowerCase();
    return allowed.length > 0 && allowed.includes(email);
  }

  async function handler(req, res, next) {
    const url = new URL(req.url, "http://localhost");
    if (url.pathname === "/admin/metrics" && req.method === "GET") {
      res.statusCode = 200;
      res.setHeader("Content-Type", "text/html; charset=utf-8");
      res.setHeader("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'");
      res.setHeader("Cache-Control", "no-store");
      return res.end(PAGE);
    }
    if (url.pathname === "/api/admin/metrics" && req.method === "GET") {
      try {
        if (!isOperator()) return json(res, 403, { error: "Operator access only. Add your account email to MIRA_ADMIN_EMAILS." });
        return json(res, 200, computeMetrics(await readEvents()));
      } catch (error) {
        return json(res, 500, { error: error.message });
      }
    }
    return next();
  }

  return {
    name: "mira-admin-api",
    apply: "serve",
    configureServer(server) { server.middlewares.use(handler); },
    configurePreviewServer(server) { server.middlewares.use(handler); },
  };
}
