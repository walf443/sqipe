#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <new-version>"
  echo "Example: $0 0.2.0"
  exit 1
fi

NEW_VERSION="$1"

# Derive the dependency version (major.minor) for inter-crate references
IFS='.' read -r major minor _patch <<< "$NEW_VERSION"
DEP_VERSION="${major}.${minor}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Bumping version to ${NEW_VERSION} (dependency version: ${DEP_VERSION})"

# Update qbey/Cargo.toml
sed -i '' "s/^version = \".*\"/version = \"${NEW_VERSION}\"/" "$ROOT_DIR/qbey/Cargo.toml"
echo "  Updated qbey/Cargo.toml"

# Update qbey-mysql/Cargo.toml (package version + qbey dependency version)
sed -i '' "s/^version = \".*\"/version = \"${NEW_VERSION}\"/" "$ROOT_DIR/qbey-mysql/Cargo.toml"
sed -i '' "s/qbey = { version = \"[^\"]*\"/qbey = { version = \"${DEP_VERSION}\"/" "$ROOT_DIR/qbey-mysql/Cargo.toml"
echo "  Updated qbey-mysql/Cargo.toml"

# Verify the workspace builds
echo ""
echo "Verifying workspace builds..."
cd "$ROOT_DIR"
cargo check --workspace --all-targets --features full

echo ""
echo "Version bumped to ${NEW_VERSION} successfully."
echo ""
echo "Next steps:"
echo "  git add -A && git commit -m 'Bump version to ${NEW_VERSION}'"
echo "  git tag v${NEW_VERSION}"
echo "  git push origin main --tags"
