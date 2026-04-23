# Pulse Parse desktop app

Tauri v2 desktop shell for Pulse Parse.

## What it does

- loads an Apple Health `export.xml` directly into one app-managed SQLite dataset
- rebuilds that desktop-owned dataset with the in-process Rust backend
- stores metadata for the desktop dataset in local desktop app state so the app can reopen it automatically
- filters by a single day or date range and workout type with a guided picker that covers more suggested activity types and accepts custom entries
- loads overview metrics, activity breakdowns, trend charts, a searchable workout overview list, selected workout detail, and workout metric drilldown modals
- exports filtered full JSON, summary JSON, or CSV output

## Local development

From the repository root, install the desktop dependencies:

```bash
cd desktop
pnpm install
```

Run the desktop app:

```bash
pnpm run tauri:dev
```

Build the macOS app bundle:

```bash
pnpm run tauri:build
```

The packaged `.app` is written to:

```bash
desktop/src-tauri/target/release/bundle/macos/Pulse Parse.app
```
