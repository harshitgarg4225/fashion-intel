# Mira — Founder Launch Playbook

Goal: **prove MVP fitness with real users in 14 days.** Not "get big" — get *evidence*: do early-20s women who try Mira come back and journal, and do they pull friends in?

Everything below is measured on the built-in dashboard: **`/admin/metrics`** (add your Google account email to `MIRA_ADMIN_EMAILS`). No third-party analytics needed.

## The four numbers that constitute proof

| Metric (dashboard tile) | Weak | Real signal | Meaning |
| --- | --- | --- | --- |
| **Activation** (signup → first render) | <40% | **≥60%** | Onboarding works; the magic moment lands |
| **D7 retention** | <10% | **≥25%** | It's a habit, not a toy |
| **Journalers** (signup → ≥1 journal log) | <20% | **≥40%** | The *journal* positioning is true |
| **Referred share of signups** | <10% | **≥30%** | Growth loop turns without ad spend |

If after ~50 users Activation and D7 clear the bar, you have an MVP worth scaling. If Activation is high but D7 is low, the render is a novelty — double down on the journal habit (daily reminder is the top roadmap item). If Activation is low, fix onboarding before acquiring anyone else.

## Day 0 (today): deploy + self-dogfood

1. Deploy per DEPLOY.md (Railway, multi-tenant, Gemini on a billing-enabled GCP project).
2. **You are user #1.** Import your own closet, render 5 looks, judge identity quality by eye across `gemini` / `fal` / `openai`. Pick the winner. If none passes, stop — nothing else matters until renders are lovable.
3. Journal what you're wearing today. Share your week to your own WhatsApp. Click the link on your phone. Fix anything that embarrasses you.

## Days 1–3: the first 20 (hand-to-hand)

Target: 20 real users you personally onboard. Not a launch — a concierge test.

- WhatsApp message that works (personal, not broadcast):
  > "I built something and I need brutal honesty. It's an AI clothing journal — you photograph your clothes once, then tell it how you want to feel ('confident', 'date night') and it shows *you* wearing an outfit from your own closet. Takes 5 min to set up. Can I send you the link? You get free renders, and I get to watch you get confused 😄"
- Onboard each one **on a call or in person**. Watch silently. Every point of confusion is a bug — file it.
- Same evening, ask each: "Will you log what you wear tomorrow?" Then watch the dashboard to see who actually does.

## Days 4–7: the group-chat wedge

The product's natural habitat is the group chat ("what should I wear Friday?").

- Ask your 5 most engaged users to share one look or their week card into one group chat each. That's the referral loop firing for real — watch **share views → referred signups** on the dashboard.
- Post 2 Reels/TikToks (raw phone footage beats produced):
  1. "I photographed my entire closet and an AI became my stylist" — screen-record import → mood chip → render appearing.
  2. "Telling an AI how I *feel* and letting it dress me for a week" — 7 renders, day by day, ending on the weekly collage.
- Bio link goes straight to the app (the landing sells the three steps).

## Days 8–14: read the numbers, then one bet

- **≥60% activation, ≥25% D7** → scale the winning channel: campus ambassador (1 fashion-forward student per campus, custom invite link each, bonus renders per activated referral — the referral system already tracks it), or Product Hunt/r/femalefashionadvice if traffic skews international.
- **Journalers high, renders low** → lead marketing with the journal + collage, renders become the premium hook.
- **Retention weak** → stop acquiring; interview the 5 users who churned ("what would have brought you back Tuesday morning?"), build the daily reminder, re-test on the next 20.

## Positioning (use everywhere)

- One-liner: **"Mira — your AI clothing journal. Dress how you feel, remember what you wore."**
- Never say "virtual try-on" (mall-tech connotation) or "wardrobe management" (chore connotation). Say *journal*, *stylist*, *feeling*.
- The share card IS the ad. Every asset ends on a share page or collage.

## Honest gaps a VC will poke (know your answers)

1. **Moat**: none in the model layer (everyone can call Gemini). The moat thesis is the *journal* — wear history + verdicts + style profile become switching costs. Prove it with D30 retention and "most-worn" stats users screenshot.
2. **Unit economics**: ~$1/acquired user in free renders at Gemini prices, ₹399+ packs on the other side. Fine at seed scale; watch renders-per-retained-user.
3. **Platform risk**: gpt-image/Gemini could ship consumer try-on natively. Answer: they won't own the closet + journal graph; speed and niche love win the wedge.
4. **Single-operator infra**: JSON files + one Railway node ≈ first few thousand users. The Postgres/object-storage migration is roadmapped, not needed for proof.
5. **Trust**: personal photos + AI = one bad story kills a young brand. Deletion, isolation, and audit logs exist; keep them sacred.
