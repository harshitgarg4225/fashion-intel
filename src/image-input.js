// Shared image intake for every upload surface (reference photo, garment
// import, journal logs, inspiration photos).
//
// Why this exists: filtering on `file.type.startsWith("image/")` silently
// drops files the browser reports with an empty MIME type — which is exactly
// what Chrome does for HEIC, the default iPhone/macOS photo format. Users
// picked a photo and nothing happened at all. Never fail silently again.
//
// It also decodes and re-encodes client-side, which:
//   - converts anything the browser can read (incl. HEIC on Safari) to JPEG,
//     so the server never has to decode exotic formats;
//   - applies EXIF orientation, so portrait phone photos aren't sideways;
//   - shrinks a 12 MB camera photo to a few hundred KB, making uploads fast
//     and keeping payloads well under the server's body cap.

const MAX_DIMENSION = 2048;
const JPEG_QUALITY = 0.9;

const looksLikeHeic = (file) => /\.(heic|heif)$/i.test(file?.name || "") || /heic|heif/i.test(file?.type || "");

function decodeError(file) {
  if (looksLikeHeic(file)) {
    return "This looks like a HEIC photo, which this browser can't read. Open it in Photos and export as JPEG — or on iPhone set Settings → Camera → Formats → Most Compatible, then try again.";
  }
  return `Could not read "${file?.name || "that file"}". Try a JPEG or PNG.`;
}

async function decode(file) {
  // createImageBitmap is the fast path and honours EXIF orientation.
  try {
    return await createImageBitmap(file, { imageOrientation: "from-image" });
  } catch {}
  // Some browsers can render a format in <img> that createImageBitmap refuses.
  const url = URL.createObjectURL(file);
  try {
    return await new Promise((resolve, reject) => {
      const image = new Image();
      image.onload = () => resolve(image);
      image.onerror = () => reject(new Error("decode failed"));
      image.src = url;
    });
  } finally {
    URL.revokeObjectURL(url);
  }
}

// Returns a JPEG data URL, or throws an Error whose message is safe to show.
export async function prepareImageFile(file, { maxDimension = MAX_DIMENSION } = {}) {
  if (!file) throw new Error("No photo selected.");

  let source;
  try {
    source = await decode(file);
  } catch {
    throw new Error(decodeError(file));
  }

  const width = source.width || source.naturalWidth;
  const height = source.height || source.naturalHeight;
  if (!width || !height) throw new Error(decodeError(file));

  const scale = Math.min(1, maxDimension / Math.max(width, height));
  const canvas = document.createElement("canvas");
  canvas.width = Math.max(1, Math.round(width * scale));
  canvas.height = Math.max(1, Math.round(height * scale));
  const context = canvas.getContext("2d");
  // Transparent PNGs would otherwise flatten to black once encoded as JPEG.
  context.fillStyle = "#ffffff";
  context.fillRect(0, 0, canvas.width, canvas.height);
  context.drawImage(source, 0, 0, canvas.width, canvas.height);
  source.close?.();

  const dataUrl = canvas.toDataURL("image/jpeg", JPEG_QUALITY);
  if (!dataUrl.startsWith("data:image/jpeg")) throw new Error(decodeError(file));
  return dataUrl;
}

// Every file picker in the app uses this: image/* alone hides HEIC in macOS
// and some Android pickers, so the extensions are named explicitly.
export const IMAGE_ACCEPT = "image/*,.heic,.heif";
