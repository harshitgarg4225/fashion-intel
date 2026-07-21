// Deliver a share URL through the best available channel, reporting what
// actually happened — never claim "copied" when nothing was.
export async function deliverShare({ title, url }) {
  if (navigator.share) {
    try {
      await navigator.share({ title, url });
      return { status: "shared" };
    } catch (error) {
      if (error?.name === "AbortError") return { status: "cancelled" };
      // NotAllowedError etc. — fall through to the clipboard.
    }
  }
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(url);
      return { status: "copied" };
    } catch {}
  }
  return { status: "manual", url };
}
