desktop_dir := "desktop"

# Show available recipes.
default:
    @just --list

# Install desktop JavaScript dependencies.
install:
    cd {{desktop_dir}} && npm install

# Run cargo check for the Rust workspace.
check:
    cargo check --workspace

# Build the Rust workspace.
build:
    cargo build --workspace

# Run Rust workspace tests.
test:
    cargo test --workspace

# Format Rust and desktop frontend code.
fmt:
    cargo fmt --all
    cd {{desktop_dir}} && npm run format

# Check Rust formatting without changing files.
fmt-check:
    cargo fmt --all --check

# Run clippy across the Rust workspace.
clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Run the repository linters.
lint:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cd {{desktop_dir}} && pnpm run lint

# Auto-fix desktop lint issues where Biome can do so.
lint-fix:
    cd {{desktop_dir}} && pnpm run lint:fix

# Build the desktop frontend bundle.
web-build:
    cd {{desktop_dir}} && pnpm run build

# Start the desktop frontend dev server.
web-dev:
    cd {{desktop_dir}} && pnpm run dev

# Preview the built desktop frontend.
preview:
    cd {{desktop_dir}} && pnpm run preview

# Run the full Tauri desktop app in development mode.
dev:
    cd {{desktop_dir}} && pnpm run tauri:dev

# Build the desktop app bundle for the current platform.
bundle:
    cd {{desktop_dir}} && pnpm run tauri:build

# Run the common local verification flow.
verify:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace
    cd {{desktop_dir}} && pnpm run lint
    cd {{desktop_dir}} && pnpm run build
