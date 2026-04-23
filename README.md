# Pulse Parse

Pulse Parse is a SQLite-backed Apple Health export parsing and workout analysis toolkit with a Tauri desktop app and shared Rust crates.

## What it does

- preprocesses Apple Health `export.xml` files so malformed DTD content and invalid control characters do not break parsing
- ingests workouts and records into a local SQLite database
- links workouts to overlapping Apple Health records so exports include corresponding workout data
- inspects workout coverage, summarizes activity over a time window, and exports workout bundles as JSON or CSV

## Repository layout

This is a Rust workspace with a Tauri desktop app nested under `desktop/`, not a single-package Tauri repo:

- `desktop/` contains the Vite/React frontend and the Tauri app package
- `desktop/src-tauri/` contains the Tauri host, commands, and bundling config
- `crates/health-core` contains shared request/event contracts and dataset metadata types
- `crates/health-store` contains the SQLite ingest, query, and export layer
- `crates/health-service` contains the desktop-facing service layer used by Tauri

That means the desktop app itself follows the usual Tauri split (`src/` plus `src-tauri/`), while the backend logic is factored into shared Rust crates.

## Desktop app

The repository includes a Tauri v2 desktop app in `desktop/` with an in-process Rust backend built from the shared workspace crates.

The Rust desktop backend is organized in `crates/`:

- `health-core` owns the desktop request/event contracts and shared dataset metadata types
- `health-store` owns app-managed dataset path conventions and dataset state persistence
- `health-service` owns the desktop-facing ingest, dashboard, workout detail, and export use cases used by Tauri

Prerequisites for local desktop development and packaging:

```bash
cd desktop
pnpm install
```

If you use [`just`](https://github.com/casey/just), the repository root includes shortcuts for the common flows:

```bash
just install
just check
just test
just lint
just fmt
just dev
just bundle
```

Run `just --list` to see the full set of available recipes.

Run the desktop app in development mode:

```bash
pnpm run tauri:dev
```

Build the desktop bundle for your platform:

```bash
pnpm run tauri:build
```

If you build on Linux, install the system packages required by Tauri/WebKitGTK first. If you build on Windows, make sure WebView2 is available on the target machine.

The desktop app supports:

- loading an Apple Health `export.xml` directly into one app-managed SQLite dataset
- rebuilding the desktop dataset with inline progress reporting
- reopening the desktop dataset from local app state without reselecting a DB path
- filtering by a single day or date range plus workout type, including broader suggested types and custom activity-type entries
- viewing overview metrics, activity breakdowns, trends, a searchable workout overview list, selected workout detail, and workout metric drilldown modals
- exporting full JSON, condensed summary JSON, or CSV directories

The desktop app is self-contained on the backend side: it uses the bundled Rust service layer directly.

## Platform support

The desktop app is structured to be cross-platform:

- app-managed datasets and config use Tauri's platform-specific app data and config directories
- SQLite is bundled through `rusqlite`, so the app does not depend on a system SQLite install
- the bundle config includes Windows icons (`.ico`) plus PNG assets used by Linux and macOS

The source layout targets macOS, Linux, and Windows, but the repository does not include a CI matrix that exercises those platforms automatically.
