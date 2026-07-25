# Aegis Dashboard

React + TypeScript + Tailwind, talking to the aegisd API over REST and to
`/ws/events` over WebSocket for live updates. See the [repo root
README](../README.md) for the full project.

## Development

```bash
npm install
npm run dev
```

The dev server proxies `/api` and `/ws` to `http://localhost:8080` (see
`vite.config.ts`), so run `go run ./cmd/aegisd` alongside it.

## Production build

```bash
npm run build
```

Outputs to `dist/`, which `go:embed` (see `../web/embed.go`) bakes into the
`aegisd` binary — this is why `dist/` is committed rather than gitignored.
