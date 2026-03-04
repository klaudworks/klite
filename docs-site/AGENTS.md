# Docs Site Agent Context

This directory contains the Astro/Starlight project that builds the klite documentation site.

## Architecture

- **`docs-site/`** — Astro/Starlight project (config, dependencies, build tooling)
- **`docs/`** (parent directory) — Pure Markdown content files
- Content is linked via symlink: `src/content/docs → ../../../docs`

Docs content lives in `../docs/` as plain Markdown. This directory only contains build infrastructure.

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

- Docs pages go in `../docs/`, not in this directory.
- Use `.md` files only (not `.mdx`) — `.mdx` files outside the project root cannot resolve Starlight component imports.
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
