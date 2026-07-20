<div align="center">

# Fashion Intel

Your wardrobe, digitized and styled with AI.

[![License: MIT](https://img.shields.io/badge/license-MIT-191919?style=flat-square)](LICENSE)
[![Node 22+](https://img.shields.io/badge/node-22%2B-191919?style=flat-square)](package.json)

</div>

Fashion Intel turns photos of your clothes into an organized digital closet, then styles complete outfits from it and renders them on you.

- **Import** — drop, paste, or upload any photo. AI vision detects every garment, extracts a clean transparent product cutout, and generates an editorial photo of the piece modeled on you.
- **Closet** — browse everything you own by category, search by name, tag, or category, and see your wardrobe's dominant color palette at a glance.
- **Outfit Studio** — one click curates a complete look (top, bottom, optional layer, shoes, accessory) from your real closet with an LLM stylist, explains why it works, and renders a square lookbook photo of you wearing it. Give it an optional direction like "smart-casual dinner" or "warm evening".

Everything stays local: originals, cutouts, modeled photos, outfits, and the JSON database live in `data/` on your machine.

## Quick start

```bash
git clone https://github.com/harshitgarg4225/fashion-intel.git
cd fashion-intel
npm install
cp .env.example .env
npm run dev
```

Then:

1. Add `OPENAI_API_KEY` to `.env` (required for all image generation).
2. Put a clear PNG photo of yourself at `data/model-reference.png`.
3. Open [localhost:5173](http://localhost:5173) and drop in a photo of your clothes.

Optionally add `ANTHROPIC_API_KEY` to run the outfit-curation stylist on Claude — image rendering still uses OpenAI gpt-image, which is the only part that requires an OpenAI key.

## How it works

| Step | Model | What happens |
| --- | --- | --- |
| Detect | `OPENAI_VISION_MODEL` | Finds every garment in a photo with bounding boxes, names, colors, and tags |
| Extract | `OPENAI_IMAGE_MODEL` | Reconstructs a clean product cutout on a chroma key, then removes the background locally |
| Model | `OPENAI_IMAGE_MODEL` | Renders the piece worn by you, using your reference photo |
| Style | Claude or OpenAI | Curates one coherent outfit from your closet with color-harmony and silhouette rules |
| Lookbook | `OPENAI_IMAGE_MODEL` | Renders the full outfit on you as a square editorial photo |

Every generation step is reviewable: approve, reject, or regenerate with a correction before anything enters your closet.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `OPENAI_API_KEY` | Required | Image generation and default vision/stylist |
| `OPENAI_VISION_MODEL` | `gpt-5.4-mini` | Garment detection |
| `OPENAI_IMAGE_MODEL` | `gpt-image-2` | Cutouts, modeled photos, outfit renders |
| `OPENAI_IMAGE_QUALITY` | `high` | Image quality |
| `ANTHROPIC_API_KEY` | Optional | Runs the outfit stylist on Claude when set |
| `ANTHROPIC_MODEL` | `claude-sonnet-5` | Claude stylist model |
| `WARDROBE_STYLIST_PROVIDER` | auto | Force `openai` or `anthropic` for the stylist |
| `WARDROBE_MODEL_REFERENCE` | `data/model-reference.png` | Your reference photo |
| `WARDROBE_DATA_DIR` | `data` | Local storage location |

## Import at scale with agent skills

The repo bundles two agent skills under `.agents/skills/` for bulk work from a coding agent (Codex, Claude Code, etc.):

- `import-clothes` — sweep a whole photo folder or camera roll, extract and model every piece, and write them into the closet database.
- `generate-outfits` — batch-create a verified, modeled lookbook from the closet.

## Credits

Built on [tandpfun/wardrobe](https://github.com/tandpfun/wardrobe) by Thijs Simonian (MIT), the project behind the viral "AI organized my entire wardrobe" post. Fashion Intel extends it with an in-app Outfit Studio (outfit curation, rendering, and a lookbook gallery — no external agent required), an optional Claude-powered stylist, closet search, category counts, and a wardrobe color palette.

## License

[MIT](LICENSE)
