import { createHmac, timingSafeEqual } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { atomicJson } from "./import-job-api.mjs";
import { currentUser, isMultiTenant } from "./tenant.mjs";

// Razorpay credit packs. Active only in multi-tenant mode with
// RAZORPAY_KEY_ID/RAZORPAY_KEY_SECRET set. Flow: create order server-side,
// Razorpay Checkout on the client, verify the payment signature server-side,
// then credit the user's account. A signed webhook is the crediting fallback.
export const CREDIT_PACKS = [
  { id: "starter", name: "Starter", credits: 50, amountPaise: 39900, label: "₹399" },
  { id: "wardrobe", name: "Wardrobe", credits: 150, amountPaise: 99900, label: "₹999" },
  { id: "studio", name: "Studio", credits: 400, amountPaise: 199900, label: "₹1,999" },
];

function json(res, status, value) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(JSON.stringify(value));
}

async function rawBody(req, limit = 256 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw Object.assign(new Error("Request body too large"), { status: 413 });
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function safeEqual(a, b) {
  const bufferA = Buffer.from(String(a));
  const bufferB = Buffer.from(String(b));
  return bufferA.length === bufferB.length && timingSafeEqual(bufferA, bufferB);
}

export function billingApi(options = {}) {
  const setting = (name, fallback = "") => options.env?.[name] || process.env[name] || fallback;
  let ordersFile = null;
  let usersFile = null;
  const keyId = () => setting("RAZORPAY_KEY_ID").trim();
  const keySecret = () => setting("RAZORPAY_KEY_SECRET").trim();
  const configured = () => Boolean(keyId() && keySecret());
  const apiBase = () => setting("RAZORPAY_API_BASE_URL", "https://api.razorpay.com/v1").replace(/\/$/, "");

  async function loadJson(file, fallback) {
    try { return JSON.parse(await readFile(file, "utf8")); }
    catch (error) { if (error.code === "ENOENT") return fallback; throw error; }
  }

  // Idempotent: credits are granted exactly once per order, whether the
  // verify endpoint or the webhook lands first.
  async function creditOrder(orderId, paymentId, via) {
    const orders = await loadJson(ordersFile, []);
    const order = orders.find((entry) => entry.orderId === orderId);
    if (!order || order.status === "paid") return order || null;
    order.status = "paid";
    order.paymentId = paymentId;
    order.paidVia = via;
    order.paidAt = new Date().toISOString();
    await atomicJson(ordersFile, orders);
    const users = await loadJson(usersFile, {});
    if (users[order.userId]) {
      users[order.userId].credits = (Number(users[order.userId].credits) || 0) + order.credits;
      await atomicJson(usersFile, users);
    }
    return order;
  }

  async function handler(req, res, next) {
    const url = new URL(req.url, "http://localhost");
    if (!url.pathname.startsWith("/api/billing/")) return next();
    if (!isMultiTenant(options.env)) return json(res, 404, { error: "Billing is available on hosted deployments only." });
    try {
      if (url.pathname === "/api/billing/packs" && req.method === "GET") {
        return json(res, 200, { configured: configured(), keyId: configured() ? keyId() : null, packs: CREDIT_PACKS.map(({ id, name, credits, amountPaise, label }) => ({ id, name, credits, amountPaise, label })) });
      }
      if (url.pathname === "/api/billing/webhook" && req.method === "POST") {
        const secret = setting("RAZORPAY_WEBHOOK_SECRET").trim();
        if (!secret) return json(res, 503, { error: "Webhook secret not configured" });
        const raw = await rawBody(req);
        const expected = createHmac("sha256", secret).update(raw).digest("hex");
        if (!safeEqual(req.headers["x-razorpay-signature"] || "", expected)) return json(res, 400, { error: "Invalid signature" });
        const event = JSON.parse(raw.toString("utf8"));
        if (event.event === "payment.captured") {
          const payment = event.payload?.payment?.entity;
          if (payment?.order_id) await creditOrder(payment.order_id, payment.id, "webhook");
        }
        return json(res, 200, { ok: true });
      }

      const user = currentUser();
      if (!user) return json(res, 401, { error: "auth_required" });

      if (url.pathname === "/api/billing/order" && req.method === "POST") {
        if (!configured()) return json(res, 503, { error: "Payments are not configured yet." });
        const input = JSON.parse((await rawBody(req)).toString("utf8") || "{}");
        const pack = CREDIT_PACKS.find((entry) => entry.id === input.packId);
        if (!pack) return json(res, 400, { error: "Pick a valid credit pack." });
        const response = await fetch(`${apiBase()}/orders`, {
          method: "POST",
          headers: {
            Authorization: `Basic ${Buffer.from(`${keyId()}:${keySecret()}`).toString("base64")}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            amount: pack.amountPaise,
            currency: "INR",
            receipt: `mira_${user.id}_${Date.now()}`,
            notes: { userId: user.id, packId: pack.id, credits: String(pack.credits) },
          }),
        });
        const order = await response.json().catch(() => ({}));
        if (!response.ok || !order.id) return json(res, 502, { error: order.error?.description || "Could not start the payment." });
        const orders = await loadJson(ordersFile, []);
        orders.push({ orderId: order.id, userId: user.id, packId: pack.id, credits: pack.credits, amountPaise: pack.amountPaise, status: "created", createdAt: new Date().toISOString() });
        await atomicJson(ordersFile, orders);
        return json(res, 200, { orderId: order.id, keyId: keyId(), amount: pack.amountPaise, currency: "INR", packName: pack.name, credits: pack.credits });
      }

      if (url.pathname === "/api/billing/verify" && req.method === "POST") {
        const input = JSON.parse((await rawBody(req)).toString("utf8") || "{}");
        const { razorpay_order_id: orderId, razorpay_payment_id: paymentId, razorpay_signature: signature } = input;
        if (!orderId || !paymentId || !signature) return json(res, 400, { error: "Missing payment details." });
        const orders = await loadJson(ordersFile, []);
        const order = orders.find((entry) => entry.orderId === orderId && entry.userId === user.id);
        if (!order) return json(res, 404, { error: "Order not found." });
        const expected = createHmac("sha256", keySecret()).update(`${orderId}|${paymentId}`).digest("hex");
        if (!safeEqual(signature, expected)) return json(res, 400, { error: "Payment could not be verified." });
        const credited = await creditOrder(orderId, paymentId, "verify");
        return json(res, 200, { ok: true, credits: credited?.credits || 0 });
      }

      return json(res, 404, { error: "Not found" });
    } catch (error) {
      return json(res, error.status || 500, { error: error.message || "Internal server error" });
    }
  }

  return {
    name: "mira-billing-api",
    apply: "serve",
    configResolved(config) {
      const dataDir = path.resolve(config.root, setting("WARDROBE_DATA_DIR", "data"));
      ordersFile = path.join(dataDir, "billing.json");
      usersFile = path.join(dataDir, "users.json");
    },
    configureServer(server) { server.middlewares.use(handler); },
    configurePreviewServer(server) { server.middlewares.use(handler); },
  };
}
