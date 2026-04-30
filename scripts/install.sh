#!/usr/bin/env bash
set -euo pipefail

REPO="opay-bigdata/spark-cli"
BIN_DIR="${SPARK_CLI_BIN_DIR:-$HOME/.local/bin}"

mkdir -p "$BIN_DIR"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) ARCH=amd64 ;;
  arm64|aarch64) ARCH=arm64 ;;
  *) echo "unsupported arch: $ARCH" >&2; exit 1 ;;
esac

VERSION="${SPARK_CLI_VERSION:-$(curl -fsSL https://api.github.com/repos/$REPO/releases/latest | grep -oE '"tag_name":\s*"[^"]+"' | head -1 | sed 's/.*"\([^"]*\)"$/\1/')}"
if [[ -z "$VERSION" ]]; then
  echo "could not resolve latest version; set SPARK_CLI_VERSION=vX.Y.Z" >&2
  exit 1
fi
VERSION="${VERSION#v}"

URL="https://github.com/$REPO/releases/download/v${VERSION}/spark-cli_${VERSION}_${OS}_${ARCH}.tar.gz"
echo "Downloading $URL"
TMP="$(mktemp -d)"
curl -fsSL "$URL" -o "$TMP/spark-cli.tar.gz"
tar -xzf "$TMP/spark-cli.tar.gz" -C "$TMP"
install -m 0755 "$TMP/spark-cli" "$BIN_DIR/spark-cli"

# install agent skill
SKILL_DIR="${SPARK_CLI_SKILL_DIR:-$HOME/.claude/skills/spark}"
mkdir -p "$SKILL_DIR"
install -m 0644 "$TMP/.claude/skills/spark/SKILL.md" "$SKILL_DIR/SKILL.md" || true

rm -rf "$TMP"
echo "Installed: $BIN_DIR/spark-cli"
echo "Skill:     $SKILL_DIR/SKILL.md"
echo
echo "Make sure $BIN_DIR is on your PATH."
