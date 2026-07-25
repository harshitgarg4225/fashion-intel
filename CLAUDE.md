# CLAUDE.md

Read **[AGENTS.md](AGENTS.md)** first — it is the canonical briefing for any AI agent working on this
codebase: architecture (the backend is Vite middleware plugins, not a separate server), the
AsyncLocalStorage tenancy rule, provider swapping, the mock-server testing pattern, hard-won traps,
and where to build next.

Quick reference:

```bash
npm run dev      # 127.0.0.1:5173
npm test         # node --test tests/*.test.mjs
npm run build    # must pass before committing
```

Definition of done: build green, tests green, the affected flow exercised over real HTTP against mock
providers, `data/` and mock files removed. Never commit `data/` or `.env` — they hold users' personal
photos and OAuth tokens.
