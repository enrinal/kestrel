#!/bin/bash
# Assembles Kestrel.app from the SwiftPM executables.
#
# This machine has Command Line Tools only (no xcodebuild), so the bundle is
# staged by hand. Usage: Scripts/build-app.sh [debug|release]
#
# The bundle is made self-contained: every Homebrew dylib it reaches is copied
# into Contents/Frameworks and the references rewritten to @rpath. Without that
# the app only runs on a Mac that has `brew install librdkafka`, which is not a
# thing to ask of someone who just opened a DMG.
set -euo pipefail

CONFIG="${1:-debug}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

swift build -c "$CONFIG"

BIN_DIR="$(swift build -c "$CONFIG" --show-bin-path)"
APP="$ROOT/build/Kestrel.app"
FRAMEWORKS="$APP/Contents/Frameworks"

rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources" "$APP/Contents/Helpers" "$FRAMEWORKS"

# The target is KestrelApp; the file inside the bundle stays Kestrel, which is
# what Info.plist's CFBundleExecutable names. The target was renamed because the
# CLI product is `kestrel`, and on a case-insensitive volume two products whose
# names differ only in case are one file in .build.
cp "$BIN_DIR/KestrelApp" "$APP/Contents/MacOS/Kestrel"

# The CLI ships inside the bundle, so installing the app installs both. It goes
# in Helpers rather than MacOS for the same case-insensitivity reason: `kestrel`
# and `Kestrel` cannot live in one directory on this filesystem.
cp "$BIN_DIR/kestrel" "$APP/Contents/Helpers/kestrel"

cp "$ROOT/Resources/Info.plist" "$APP/Contents/Info.plist"
if [ -f "$ROOT/Resources/AppIcon.icns" ]; then
	cp "$ROOT/Resources/AppIcon.icns" "$APP/Contents/Resources/AppIcon.icns"
else
	echo "warning: Resources/AppIcon.icns is missing; run 'swift Scripts/make-icon.swift'" >&2
fi
printf 'APPL????' > "$APP/Contents/PkgInfo"

# --- Bundle the dylibs -------------------------------------------------------

# Lists the non-system libraries a binary loads.
deps_of() {
	otool -L "$1" \
		| tail -n +2 \
		| awk '{print $1}' \
		| grep -v -e '^/usr/lib' -e '^/System' -e '^@' \
		|| true
}

# Copies a library in and points every reference at @rpath, following what that
# library itself loads. librdkafka alone pulls in lz4, zstd, libssl and
# libcrypto, and libssl pulls in libcrypto again, so this has to be a graph
# walk rather than one pass over one binary.
bundle_deps() {
	local binary="$1"
	local dep name

	for dep in $(deps_of "$binary"); do
		name="$(basename "$dep")"

		if [ ! -f "$FRAMEWORKS/$name" ]; then
			# The source may itself be a symlink into a versioned Cellar path.
			cp "$(readlink -f "$dep" 2>/dev/null || echo "$dep")" "$FRAMEWORKS/$name"
			chmod u+w "$FRAMEWORKS/$name"
			install_name_tool -id "@rpath/$name" "$FRAMEWORKS/$name"
			# Recurse before rewriting, so a library pulled in only by another
			# library still gets copied.
			bundle_deps "$FRAMEWORKS/$name"
		fi

		install_name_tool -change "$dep" "@rpath/$name" "$binary"
	done
}

# Adds an rpath, ignoring the error when it is already there.
add_rpath() {
	install_name_tool -add_rpath "$2" "$1" 2>/dev/null || true
}

for binary in "$APP/Contents/MacOS/Kestrel" "$APP/Contents/Helpers/kestrel"; do
	bundle_deps "$binary"
done

# The executables sit at different depths, so they need different rpaths.
add_rpath "$APP/Contents/MacOS/Kestrel" "@executable_path/../Frameworks"
add_rpath "$APP/Contents/Helpers/kestrel" "@executable_path/../Frameworks"

# --- Sign --------------------------------------------------------------------

# Ad-hoc signature: unsigned SwiftUI bundles are refused by recent macOS, and
# install_name_tool invalidates whatever signature a binary arrived with, so
# everything is signed after being rewritten — libraries first, bundle last.
for dylib in "$FRAMEWORKS"/*.dylib; do
	[ -f "$dylib" ] && codesign --force --sign - --timestamp=none "$dylib" >/dev/null 2>&1 || true
done
codesign --force --sign - --timestamp=none "$APP/Contents/Helpers/kestrel" >/dev/null 2>&1 || true
codesign --force --sign - --timestamp=none "$APP" >/dev/null 2>&1 || true

echo "$APP"
