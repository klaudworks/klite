# Docs Site Agent Context

This directory contains the Astro/Starlight project that builds the klite documentation site.

## Architecture

- **`docs-site/`** — Astro/Starlight project (config, dependencies, build tooling)
- **`docs-site/src/content/docs/`** — Documentation content lives here (`.md` and `.mdx` files)
- **`docs/`** (repo root) — Symlink → `docs-site/src/content/docs/` for discoverability

Content lives inside the Astro project so that `.mdx` files can resolve Starlight component imports.

## Commands

```sh
pnpm install        # Install dependencies
pnpm run dev        # Start dev server (http://localhost:4321)
pnpm run build      # Production build (output: dist/)
pnpm run preview    # Preview production build
```

## Key Files

- `astro.config.mjs` — Starlight config (sidebar, title, social links)
- `src/content.config.ts` — Content collection schema (rarely needs changes)
- `package.json` — Dependencies and scripts

## Content Rules

- Docs pages go in `src/content/docs/`. The repo-root `docs/` symlink points here for discoverability.
- Use `.md` files by default. Use `.mdx` when you need Starlight components (e.g., `<Tabs>`, `<Card>`). Import with `import { Tabs, TabItem } from '@astrojs/starlight/components';`.
- Frontmatter is required: at minimum `title` and `description`.
- Sidebar is auto-generated from directory structure via `autogenerate` in `astro.config.mjs`. Add new sidebar sections there when creating new top-level directories in `docs/`.

## Deployment

Built for Cloudflare Pages. Build settings:
- Build command: `pnpm run build`
- Output directory: `dist/`
- Root directory: `docs-site/`

## Reference

When you need to look up Starlight features, configuration options, components, or customization, consult the official Starlight documentation:

**https://starlight.astro.build/getting-started/**

Use the docs-explore subagent with that URL to find specific answers about sidebar configuration, search, i18n, CSS customization, built-in components, and other Starlight capabilities.
