import { CREDIT_PACKS } from "./billing-api.mjs";

// Public legal pages — required for Razorpay activation and privacy
// compliance: Terms, Privacy, Refund/Cancellation, Contact, Pricing.
// Operator identity comes from env so the pages are truthful per deployment.
const esc = (text) => String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

function shell(title, body) {
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${esc(title)} — Mira</title><style>
  body{margin:0;background:#f8f5ef;color:#16130e;font-family:Helvetica,Arial,sans-serif;line-height:1.65}
  .wrap{max-width:680px;margin:0 auto;padding:56px 24px 96px}
  .eyebrow{font-size:11px;letter-spacing:.3em;color:#9d7b4f;text-transform:uppercase;margin:0 0 10px;text-align:center}
  h1{font-family:Georgia,serif;font-weight:600;font-size:34px;margin:0;text-align:center}
  .rule{width:56px;height:1px;background:#9d7b4f;margin:20px auto 40px}
  h2{font-family:Georgia,serif;font-weight:600;font-size:21px;margin:36px 0 10px}
  p,li{font-size:14.5px;color:#3d3a33}
  a{color:#9d7b4f}
  table{border-collapse:collapse;width:100%;margin:14px 0}
  td,th{border:1px solid #e0d9ca;padding:10px 14px;font-size:14px;text-align:left}
  .foot{margin-top:56px;text-align:center;font-size:11px;letter-spacing:.2em;color:#71695c;text-transform:uppercase}
  .foot a{margin:0 8px;color:#71695c;text-decoration:none}
  </style></head><body><div class="wrap"><p class="eyebrow">Mira</p><h1>${esc(title)}</h1><div class="rule"></div>${body}
  <p class="foot"><a href="/legal/terms">Terms</a>·<a href="/legal/privacy">Privacy</a>·<a href="/legal/refunds">Refunds</a>·<a href="/legal/pricing">Pricing</a>·<a href="/legal/contact">Contact</a></p>
  </div></body></html>`;
}

export function legalApi(options = {}) {
  const setting = (name, fallback = "") => options.env?.[name] || process.env[name] || fallback;
  const operator = () => esc(setting("MIRA_OPERATOR_NAME", "the operator of this Mira deployment"));
  const email = () => esc(setting("MIRA_SUPPORT_EMAIL", "the contact address configured by the operator"));
  const emailLink = () => setting("MIRA_SUPPORT_EMAIL") ? `<a href="mailto:${email()}">${email()}</a>` : email();

  const pages = {
    terms: () => shell("Terms of Service", `
      <p>Welcome to Mira ("the Service"), operated by ${operator()}. By creating an account or using the Service you agree to these terms.</p>
      <h2>1. The Service</h2>
      <p>Mira digitizes your wardrobe from photos you provide and uses artificial-intelligence models to catalog garments, suggest outfits, and generate images of you wearing them. Generated images are AI renderings: they approximate, and may not perfectly represent, real garments or your appearance.</p>
      <h2>2. Your account</h2>
      <p>You sign in with Google and must be at least 18 years old (or the age of digital consent in your jurisdiction). You are responsible for activity on your account. One account per person.</p>
      <h2>3. Your content</h2>
      <p>You retain all rights to the photos you upload and the images generated for you. You grant the Service only the processing rights needed to operate (detection, styling, rendering, storage). Upload only photos you have the right to use, of yourself. Do not upload unlawful content or images of others without their consent.</p>
      <h2>4. Render credits</h2>
      <p>Image generation consumes render credits. Credits are consumed only when a generation succeeds. Purchased credits do not expire and are non-transferable. See the <a href="/legal/refunds">Refund Policy</a>.</p>
      <h2>5. Acceptable use</h2>
      <p>No attempts to breach other accounts, overload the Service, generate unlawful or harmful imagery, or misrepresent generated images as authentic photographs where that could deceive.</p>
      <h2>6. Disclaimers and liability</h2>
      <p>The Service is provided "as is". To the maximum extent permitted by law, ${operator()} is not liable for indirect or consequential damages; total liability is limited to the amount you paid in the preceding three months.</p>
      <h2>7. Termination</h2>
      <p>You may delete your account at any time from within the app, which removes your data. We may suspend accounts that violate these terms.</p>
      <h2>8. Governing law and changes</h2>
      <p>These terms are governed by the laws of India. We may update these terms; material changes will be shown in the app. Continued use after changes is acceptance.</p>
      <p>Contact: ${emailLink()}</p>`),

    privacy: () => shell("Privacy Policy", `
      <p>This policy explains how ${operator()} handles your data on Mira, in line with the Digital Personal Data Protection Act, 2023 (India) and comparable regulations.</p>
      <h2>What we collect</h2>
      <ul>
        <li><strong>Account</strong>: your Google name, email, and profile photo (via Google Sign-In).</li>
        <li><strong>Your photos</strong>: images you upload or import — including photos of you and your clothing — and your reference photo.</li>
        <li><strong>Generated content</strong>: garment cutouts, modeled renders, outfits, collages.</li>
        <li><strong>Usage</strong>: wear history, preferences, generation counts, and payment records (we never see or store card details — payments are processed by Razorpay).</li>
      </ul>
      <h2>How it is used</h2>
      <p>Solely to run the Service for you: detecting garments, styling outfits, rendering images, and showing your closet, insights, and journal. We do not sell your data or use it for advertising. Nothing is public unless you explicitly create a share link, which you can revoke anytime.</p>
      <h2>Processors</h2>
      <p>To operate, your images and wardrobe metadata are sent to the AI providers configured by the operator (OpenAI and/or Anthropic and/or Google) strictly to perform the requested generation; payments are processed by Razorpay; sign-in and optional photo import use Google. Each processes data under its own terms.</p>
      <h2>Storage and retention</h2>
      <p>Your closet is stored in an isolated per-account directory on the operator's server and kept until you delete it. Session cookies are strictly functional. An audit log of AI calls is kept for cost and abuse control.</p>
      <h2>Your rights</h2>
      <p>Access, correction, and erasure: you can delete individual items, revoke shares, or delete your entire account (Sign out area → Delete account), which permanently removes your photos, renders, and records. For data export or any privacy request, contact the grievance contact below; we respond within 30 days.</p>
      <h2>Grievance contact</h2>
      <p>${operator()} — ${emailLink()}</p>`),

    refunds: () => shell("Refund & Cancellation Policy", `
      <p>Mira sells prepaid render credits — a digital service delivered instantly to your account.</p>
      <h2>Cancellation</h2>
      <p>Orders complete immediately upon successful payment; there is no post-purchase cancellation window for the order itself.</p>
      <h2>Refunds</h2>
      <ul>
        <li><strong>Unused credits</strong>: refundable on request within 7 days of purchase, pro-rated for any credits already used.</li>
        <li><strong>Failed generations never consume credits</strong> — a render that errors costs you nothing.</li>
        <li><strong>Duplicate or failed payments</strong>: refunded in full once confirmed against payment records.</li>
      </ul>
      <p>To request a refund, write to ${emailLink()} with your account email and payment reference. Approved refunds are processed to the original payment method via Razorpay, typically within 5–7 business days.</p>`),

    pricing: () => shell("Pricing", `
      <p>Every new account includes free render credits. Additional credits are available as one-time packs (prices in INR, inclusive of applicable taxes):</p>
      <table><tr><th>Pack</th><th>Render credits</th><th>Price</th></tr>
      ${CREDIT_PACKS.map((pack) => `<tr><td>${esc(pack.name)}</td><td>${pack.credits}</td><td>${esc(pack.label)}</td></tr>`).join("")}
      </table>
      <p>One credit = one successful image generation (a garment render or a full outfit render). Credits never expire.</p>`),

    contact: () => shell("Contact Us", `
      <p>Mira is operated by ${operator()}.</p>
      <p>Support and privacy requests: ${emailLink()}</p>
      <p>We aim to respond within 2 business days (support) and 30 days (privacy/data requests).</p>`),
  };

  function handler(req, res, next) {
    const url = new URL(req.url, "http://localhost");
    const match = url.pathname.match(/^\/legal\/(terms|privacy|refunds|pricing|contact)\/?$/);
    if (!match || req.method !== "GET") return next();
    res.statusCode = 200;
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.setHeader("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'");
    res.setHeader("Cache-Control", "public, max-age=3600");
    return res.end(pages[match[1]]());
  }

  return {
    name: "mira-legal-api",
    apply: "serve",
    configureServer(server) { server.middlewares.use(handler); },
    configurePreviewServer(server) { server.middlewares.use(handler); },
  };
}
