import { appendFile, readFile } from "node:fs/promises";
import path from "node:path";
import { baseDataDir, currentUser } from "./tenant.mjs";

// Product-funnel events, appended to one global JSONL. This is what proves
// (or disproves) MVP fitness: signups, activation, the journaling habit,
// shares, and referral conversion — computed in admin-api.mjs. Deliberately
// minimal: no third-party analytics, no client-side beacons, no PII beyond
// the user id already stored in users.json.
const eventsFile = () => path.join(baseDataDir() || ".", "events.jsonl");

export function trackEvent(type, props = {}) {
  const base = baseDataDir();
  if (!base) return;
  const record = { ts: new Date().toISOString(), type, userId: currentUser()?.id || props.userId || "owner", ...props };
  void appendFile(eventsFile(), `${JSON.stringify(record)}\n`).catch(() => {});
}

export async function readEvents() {
  try {
    const raw = await readFile(eventsFile(), "utf8");
    return raw.split("\n").filter(Boolean).map((line) => {
      try { return JSON.parse(line); } catch { return null; }
    }).filter(Boolean);
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
}

export function computeMetrics(events, now = new Date()) {
  const day = (ts) => ts.slice(0, 10);
  const signupAt = new Map();
  const renderUsers = new Set();
  const journalUsers = new Set();
  const totals = { signups: 0, referredSignups: 0, renders: 0, journalLogs: 0, sharesCreated: 0, shareViews: 0, referralActivations: 0 };
  const dayKeys = Array.from({ length: 14 }, (_, index) => {
    const date = new Date(now);
    date.setUTCDate(date.getUTCDate() - (13 - index));
    return date.toISOString().slice(0, 10);
  });
  const series = Object.fromEntries(dayKeys.map((key) => [key, { signups: 0, renders: 0, journalLogs: 0, shareViews: 0, activeUsers: new Set() }]));
  const eventsByUser = new Map();

  for (const event of events) {
    const bucket = series[day(event.ts)];
    // Anonymous share views are traffic, not user activity.
    const countsAsActivity = event.userId && event.type !== "share_view";
    if (bucket && countsAsActivity) bucket.activeUsers.add(event.userId);
    if (countsAsActivity) {
      if (!eventsByUser.has(event.userId)) eventsByUser.set(event.userId, []);
      eventsByUser.get(event.userId).push(event.ts);
    }
    switch (event.type) {
      case "signup":
        totals.signups += 1;
        if (event.referred) totals.referredSignups += 1;
        signupAt.set(event.userId, event.ts);
        if (bucket) bucket.signups += 1;
        break;
      case "render":
        totals.renders += 1;
        renderUsers.add(event.userId);
        if (bucket) bucket.renders += 1;
        break;
      case "journal_log":
        totals.journalLogs += 1;
        journalUsers.add(event.userId);
        if (bucket) bucket.journalLogs += 1;
        break;
      case "share_created": totals.sharesCreated += 1; break;
      case "share_view": if (bucket) bucket.shareViews += 1; totals.shareViews += 1; break;
      case "referral_activated": totals.referralActivations += 1; break;
    }
  }

  // Retention: of users who signed up ≥N+1 days ago, how many came back in
  // the [N, N+2) day window after signup?
  const retention = (offsetDays) => {
    let eligible = 0;
    let returned = 0;
    for (const [userId, ts] of signupAt) {
      const start = new Date(ts).getTime() + offsetDays * 864e5;
      const end = start + 2 * 864e5;
      if (now.getTime() < start) continue;
      eligible += 1;
      if ((eventsByUser.get(userId) || []).some((eventTs) => {
        const time = new Date(eventTs).getTime();
        return time >= start && time < end;
      })) returned += 1;
    }
    return { eligible, returned, rate: eligible ? returned / eligible : null };
  };

  return {
    totals,
    activation: { users: renderUsers.size, rate: totals.signups ? renderUsers.size / totals.signups : null },
    journalers: { users: journalUsers.size, rate: totals.signups ? journalUsers.size / totals.signups : null },
    viral: {
      referredShare: totals.signups ? totals.referredSignups / totals.signups : null,
      viewToSignup: totals.shareViews ? totals.referredSignups / totals.shareViews : null,
    },
    retention: { d1: retention(1), d7: retention(7) },
    days: dayKeys.map((key) => ({
      date: key,
      signups: series[key].signups,
      renders: series[key].renders,
      journalLogs: series[key].journalLogs,
      shareViews: series[key].shareViews,
      activeUsers: series[key].activeUsers.size,
    })),
  };
}
