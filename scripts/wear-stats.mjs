// Pure wear-history helpers shared by the review API and tests.

export function wornDaySet(outfits) {
  const days = new Set();
  for (const outfit of outfits) {
    for (const worn of outfit.wornAt || []) days.add(worn.slice(0, 10));
  }
  return days;
}

// Consecutive days with at least one logged wear, ending today — or ending
// yesterday, so the streak reads as "alive" before you've dressed today.
// extraDays lets journal entries (photo logs) count toward the streak too.
export function computeStreak(outfits, todayStr, extraDays = []) {
  const days = wornDaySet(outfits);
  for (const day of extraDays) days.add(day);
  const cursor = new Date(`${todayStr}T00:00:00Z`);
  if (!days.has(todayStr)) cursor.setUTCDate(cursor.getUTCDate() - 1);
  let streak = 0;
  while (days.has(cursor.toISOString().slice(0, 10))) {
    streak += 1;
    cursor.setUTCDate(cursor.getUTCDate() - 1);
  }
  return streak;
}
