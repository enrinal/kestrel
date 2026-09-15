#!/bin/bash
# Builds dist/Kestrel.dmg from a release bundle.
#
# Usage: Scripts/make-dmg.sh
#
# A staging folder plus `hdiutil create` is all this needs: the disk image holds
# Kestrel.app and a symlink to /Applications, so the image can be installed by
# dragging one onto the other.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VERSION="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' Resources/Info.plist)"
APP="$ROOT/build/Kestrel.app"
STAGE="$ROOT/build/dmg-stage"
DMG="$ROOT/dist/Kestrel.dmg"

# Always build the bundle the image is made of, rather than trusting whatever
# build/ happens to hold — a DMG shipped from a stale debug build is the kind of
# mistake that is invisible until someone else opens it.
echo "building the release bundle..."
Scripts/build-app.sh release >/dev/null

[ -d "$APP" ] || { echo "error: $APP is missing" >&2; exit 1; }

rm -rf "$STAGE" "$DMG"
mkdir -p "$STAGE" "$ROOT/dist"

cp -R "$APP" "$STAGE/Kestrel.app"
ln -s /Applications "$STAGE/Applications"

# A short note in the image, because an unsigned app cannot simply be opened:
# Gatekeeper refuses a double-click and the right-click route is not obvious.
cat > "$STAGE/Read Me.txt" <<'NOTE'
Kestrel — a native macOS Kafka explorer

Installing
  Drag Kestrel.app onto the Applications folder in this window.

Opening it the first time
  Kestrel is signed ad-hoc, not with an Apple Developer ID, so macOS will
  refuse a plain double-click and offer only Move to Bin.

  Right-click (or Control-click) Kestrel.app and choose Open, then confirm.
  macOS remembers the decision, so this is needed once.

  If the Open option does not appear, run this in Terminal after copying the
  app to Applications:

      xattr -dr com.apple.quarantine /Applications/Kestrel.app

The command line tool
  A `kestrel` CLI ships inside the bundle. To put it on your PATH:

      sudo ln -sf /Applications/Kestrel.app/Contents/Helpers/kestrel \
          /usr/local/bin/kestrel

  Then `kestrel help`. It reads the same saved clusters the app does.

Requirements
  macOS 14 or newer, Apple silicon. Nothing else: the Kafka client library
  and its dependencies are inside the bundle.
NOTE

echo "creating the image..."
# UDZO is the compressed read-only format, which is what a distributable image
# should be; the volume name is what appears in the Finder sidebar when mounted.
hdiutil create \
	-volname "Kestrel $VERSION" \
	-srcfolder "$STAGE" \
	-ov \
	-format UDZO \
	-quiet \
	"$DMG"

rm -rf "$STAGE"

SIZE="$(du -h "$DMG" | cut -f1 | tr -d ' ')"
echo "$DMG ($SIZE)"
