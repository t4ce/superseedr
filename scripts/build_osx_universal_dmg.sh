#!/bin/bash

# SPDX-FileCopyrightText: 2025 The superseedr Contributors
# SPDX-License-Identifier: GPL-3.0-or-later

set -e

# Usage: ./build_osx_universal_dmg.sh <VERSION> <SUFFIX> <CERT_NAME|--unsigned|-> [CARGO_FLAGS...]

INPUT_VERSION=$1
NAME_SUFFIX=$2
SIGNING_CERT_NAME=$3
shift 3
CARGO_FLAGS="$@"

if [ -z "${SIGNING_CERT_NAME}" ]; then
    echo "::error:: Missing Developer ID Application certificate name. Pass '--unsigned' or '-' for a local build."
    exit 1
fi

APP_CERT_NAME="${SIGNING_CERT_NAME/Installer/Application}"
UNSIGNED_BUILD=false
CODESIGN_TIMESTAMP_ARGS=(--timestamp)

if [ "${SIGNING_CERT_NAME}" = "-" ] || [ "${SIGNING_CERT_NAME}" = "--unsigned" ]; then
    UNSIGNED_BUILD=true
    APP_CERT_NAME="(unsigned local build)"
    CODESIGN_TIMESTAMP_ARGS=()
fi

ENTITLEMENTS_PATH="target/entitlements.plist"
echo "Creating entitlements file at ${ENTITLEMENTS_PATH}..."
mkdir -p target
cat > "${ENTITLEMENTS_PATH}" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>com.apple.security.cs.allow-jit</key>
    <false/>
    <key>com.apple.security.cs.allow-unsigned-executable-memory</key>
    <false/>
</dict>
</plist>
EOF

APP_NAME="superseedr"
BINARY_NAME="superseedr"
HANDLER_APP_NAME="superseedr"
BUNDLE_IDENTIFIER="com.github.jagalite.superseedr"
ICON_FILE_PATH="assets/app_icon.icns"
ICON_FILE_STEM="app_icon"
ICON_FILE_NAME="app_icon.icns"
DMG_BACKGROUND_SOURCE="assets/dmg_background.png"
APPLICATIONS_ICON_FILE="/System/Library/CoreServices/CoreTypes.bundle/Contents/Resources/ApplicationsFolderIcon.icns"
DMG_WINDOW_WIDTH=760
DMG_WINDOW_HEIGHT=428

if [ ! -f "$ICON_FILE_PATH" ]; then
    echo "::error:: Icon file not found at ${ICON_FILE_PATH}"
    exit 1
fi

if [ ! -f "$DMG_BACKGROUND_SOURCE" ]; then
    echo "::error:: DMG background file not found at ${DMG_BACKGROUND_SOURCE}"
    exit 1
fi

if [ -z "$INPUT_VERSION" ]; then
    VERSION=$(git rev-parse --short HEAD)
else
    VERSION=$(echo "$INPUT_VERSION" | sed 's/^v//')
fi

TUI_BINARY_SOURCE_ARM64="target/aarch64-apple-darwin/release/${BINARY_NAME}"
TUI_BINARY_SOURCE_X86_64="target/x86_64-apple-darwin/release/${BINARY_NAME}"

HANDLER_STAGING_DIR="target/handler_staging_${NAME_SUFFIX}"
HANDLER_APP_PATH="${HANDLER_STAGING_DIR}/${HANDLER_APP_NAME}.app"
HANDLER_SCRIPT_PATH="${HANDLER_STAGING_DIR}/main.applescript"
BUNDLED_BINARY_PATH="${HANDLER_APP_PATH}/Contents/Resources/${BINARY_NAME}"

UNIVERSAL_STAGING_DIR="target/universal_staging_${NAME_SUFFIX}"
UNIVERSAL_BINARY_PATH="${UNIVERSAL_STAGING_DIR}/${BINARY_NAME}"

if [ "$NAME_SUFFIX" == "private" ]; then
  DMG_NAME="${APP_NAME}-${VERSION}-private-universal-macos.dmg"
else
  DMG_NAME="${APP_NAME}-${VERSION}-universal-macos.dmg"
fi

DMG_OUTPUT_DIR="target/release"
DMG_OUTPUT_PATH="${DMG_OUTPUT_DIR}/${DMG_NAME}"
DMG_STAGING_DIR="target/dmg_staging_${NAME_SUFFIX}"
DMG_TEMP_PATH="${DMG_OUTPUT_DIR}/${APP_NAME}-${VERSION}-${NAME_SUFFIX}-rw.dmg"
DMG_LAYOUT_PATH="${DMG_OUTPUT_DIR}/${APP_NAME}-${VERSION}-${NAME_SUFFIX}-layout.dmg"
DMG_MOUNT_DIR="target/dmg_mount_${NAME_SUFFIX}"
DMG_BACKGROUND_PATH="target/dmg_background_${NAME_SUFFIX}.png"

echo "--- Build Configuration (Universal DMG) ---"
echo "Version/Identifier: ${VERSION}"
echo "Build Type (Suffix): ${NAME_SUFFIX}"
echo "App Signer: ${APP_CERT_NAME}"
echo "Unsigned Build: ${UNSIGNED_BUILD}"
echo "DMG Output: ${DMG_OUTPUT_PATH}"
echo "-------------------------------------------"

verify_code_signature() {
    local path="$1"
    local verify_flags=("${@:2}")

    echo "Verifying code signature: ${path}"
    codesign --verify "${verify_flags[@]}" --strict --verbose=4 "${path}"
    codesign -dv --verbose=4 "${path}"
}

sign_app_bundle() {
    local app_path="$1"
    local binary_path="${app_path}/Contents/Resources/${BINARY_NAME}"

    echo "Signing bundled binary with Developer ID and Hardened Runtime: ${binary_path}"
    codesign -s "${APP_CERT_NAME}" \
      -v --force \
      --options runtime \
      "${CODESIGN_TIMESTAMP_ARGS[@]}" \
      --entitlements "${ENTITLEMENTS_PATH}" \
      "${binary_path}"
    verify_code_signature "${binary_path}"

    echo "Signing ${HANDLER_APP_NAME}.app with Developer ID and Hardened Runtime: ${app_path}"
    codesign -s "${APP_CERT_NAME}" \
      -v --force --deep \
      --options runtime \
      "${CODESIGN_TIMESTAMP_ARGS[@]}" \
      --entitlements "${ENTITLEMENTS_PATH}" \
      "${app_path}"
    verify_code_signature "${app_path}" --deep
}

sign_app_inside_readwrite_dmg() {
    local dmg_path="$1"
    local mount_dir="$2"
    local mounted_app_path="${mount_dir}/${HANDLER_APP_NAME}.app"
    local sign_status=0

    echo "Signing app inside finalized read-write DMG..."
    rm -rf "${mount_dir}"
    mkdir -p "${mount_dir}"
    hdiutil attach \
      -readwrite \
      -nobrowse \
      -noautoopen \
      -mountpoint "${mount_dir}" \
      "${dmg_path}"

    sign_app_bundle "${mounted_app_path}" || sign_status=$?
    hdiutil detach "${mount_dir}" >/dev/null || true
    rm -rf "${mount_dir}"

    if [ "${sign_status}" -ne 0 ]; then
        echo "::error:: App signing inside DMG failed."
        exit "${sign_status}"
    fi
}

verify_dmg_app_signature() {
    local dmg_path="$1"
    local mount_dir="$2"
    local mounted_app_path="${mount_dir}/${HANDLER_APP_NAME}.app"
    local verify_status=0

    echo "Verifying signed app inside DMG..."
    rm -rf "${mount_dir}"
    mkdir -p "${mount_dir}"
    hdiutil attach \
      -readonly \
      -nobrowse \
      -noautoopen \
      -mountpoint "${mount_dir}" \
      "${dmg_path}"

    codesign --verify --deep --strict --verbose=4 "${mounted_app_path}" || verify_status=$?
    hdiutil detach "${mount_dir}" >/dev/null || true
    rm -rf "${mount_dir}"

    if [ "${verify_status}" -ne 0 ]; then
        echo "::error:: App signature inside DMG failed verification."
        exit "${verify_status}"
    fi
}

echo "Building main TUI binary for Apple Silicon (aarch64)..."
cargo build --target aarch64-apple-darwin --release $CARGO_FLAGS

echo "Building main TUI binary for Intel (x86_64)..."
cargo build --target x86_64-apple-darwin --release $CARGO_FLAGS

if [ ! -f "${TUI_BINARY_SOURCE_ARM64}" ] || [ ! -f "${TUI_BINARY_SOURCE_X86_64}" ]; then
    echo "::error:: One or more built binaries missing. Build failed."
    ls -l target/*/release || true
    exit 1
fi

echo "Creating universal (FAT) binary with lipo..."
rm -rf "${UNIVERSAL_STAGING_DIR}"
mkdir -p "${UNIVERSAL_STAGING_DIR}"
lipo -create \
  -output "${UNIVERSAL_BINARY_PATH}" \
  "${TUI_BINARY_SOURCE_ARM64}" \
  "${TUI_BINARY_SOURCE_X86_64}"

echo "Building ${HANDLER_APP_NAME}.app programmatically..."
rm -rf "${HANDLER_STAGING_DIR}"
mkdir -p "${HANDLER_STAGING_DIR}"

echo "Creating AppleScript file: ${HANDLER_SCRIPT_PATH}"
cat > "${HANDLER_SCRIPT_PATH}" << EOF
use scripting additions

on run
    display dialog "${HANDLER_APP_NAME} runs in Terminal." & return & return & "Open Terminal and type:" & return & return & "superseedr" buttons {"OK"} default button "OK" with title "${HANDLER_APP_NAME}"
end run

on open location thisURL
    processLink(thisURL)
end open location

on open these_files
    repeat with thisFile in these_files
        processLink(POSIX path of thisFile)
    end repeat
end open

on processLink(theLink)
    set linkToProcess to theLink as text
    if linkToProcess is not "" then
        try
            set binaryPathPosix to bundledBinaryPath()
            set fullCommand to (quoted form of binaryPathPosix) & " " & (quoted form of linkToProcess)
            do shell script (fullCommand & " > /dev/null 2>&1 &")
        on error errMsg
            display dialog "${HANDLER_APP_NAME} Error: " & errMsg
        end try
    end if
end processLink

on bundledBinaryPath()
    set appPathPosix to POSIX path of (path to me)
    return appPathPosix & "Contents/Resources/${BINARY_NAME}"
end bundledBinaryPath
EOF

echo "Compiling AppleScript into app bundle: ${HANDLER_APP_PATH}"
osacompile -x -o "${HANDLER_APP_PATH}" "${HANDLER_SCRIPT_PATH}"

echo "Adding app resources..."
RESOURCES_PATH="${HANDLER_APP_PATH}/Contents/Resources"
rm -f "${RESOURCES_PATH}/droplet.icns"
rm -f "${RESOURCES_PATH}/droplets.icns"
rm -f "${RESOURCES_PATH}/applet.icns"
rm -f "${RESOURCES_PATH}/Assets.car"
cp "${ICON_FILE_PATH}" "${RESOURCES_PATH}/${ICON_FILE_NAME}"
cp "${ICON_FILE_PATH}" "${RESOURCES_PATH}/droplet.icns"
cp "${ICON_FILE_PATH}" "${RESOURCES_PATH}/droplets.icns"
cp "${ICON_FILE_PATH}" "${RESOURCES_PATH}/applet.icns"
cp "${UNIVERSAL_BINARY_PATH}" "${BUNDLED_BINARY_PATH}"
chmod 755 "${BUNDLED_BINARY_PATH}"
rm -rf "${HANDLER_APP_PATH}/Contents/_CodeSignature"

echo "Modifying Info.plist for ${HANDLER_APP_NAME}.app..."
PLIST_PATH="${HANDLER_APP_PATH}/Contents/Info.plist"

/usr/libexec/PlistBuddy -c "Delete :CFBundleIconFile" "${PLIST_PATH}" || true
/usr/libexec/PlistBuddy -c "Add :CFBundleIconFile string ${ICON_FILE_STEM}" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Delete :CFBundleIconName" "${PLIST_PATH}" || true
/usr/libexec/PlistBuddy -c "Delete :CFBundleIdentifier" "${PLIST_PATH}" || true
/usr/libexec/PlistBuddy -c "Add :CFBundleIdentifier string ${BUNDLE_IDENTIFIER}" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Delete :CFBundleName" "${PLIST_PATH}" || true
/usr/libexec/PlistBuddy -c "Add :CFBundleName string ${APP_NAME}" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Delete :CFBundleDisplayName" "${PLIST_PATH}" || true
/usr/libexec/PlistBuddy -c "Add :CFBundleDisplayName string ${APP_NAME}" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Delete :CFBundleShortVersionString" "${PLIST_PATH}" || true
/usr/libexec/PlistBuddy -c "Add :CFBundleShortVersionString string ${VERSION}" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Delete :CFBundleVersion" "${PLIST_PATH}" || true
/usr/libexec/PlistBuddy -c "Add :CFBundleVersion string ${VERSION}" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Delete :CFBundleSignature" "${PLIST_PATH}" || true
/usr/libexec/PlistBuddy -c "Add :CFBundleSignature string ????" "${PLIST_PATH}"

/usr/libexec/PlistBuddy -c "Delete :CFBundleURLTypes" "${PLIST_PATH}" || true
/usr/libexec/PlistBuddy -c "Add :CFBundleURLTypes array" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleURLTypes:0 dict" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleURLTypes:0:CFBundleTypeRole string Viewer" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleURLTypes:0:CFBundleURLName string 'Magnet URI'" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleURLTypes:0:CFBundleURLSchemes array" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleURLTypes:0:CFBundleURLSchemes:0 string magnet" "${PLIST_PATH}"

/usr/libexec/PlistBuddy -c "Delete :CFBundleDocumentTypes" "${PLIST_PATH}" || true
/usr/libexec/PlistBuddy -c "Add :CFBundleDocumentTypes array" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleDocumentTypes:0 dict" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleDocumentTypes:0:CFBundleTypeRole string Viewer" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleDocumentTypes:0:CFBundleTypeName string 'BitTorrent File'" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleDocumentTypes:0:LSHandlerRank string Owner" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleDocumentTypes:0:LSItemContentTypes array" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleDocumentTypes:0:LSItemContentTypes:0 string org.bittorrent.torrent" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleDocumentTypes:0:CFBundleTypeExtensions array" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :CFBundleDocumentTypes:0:CFBundleTypeExtensions:0 string torrent" "${PLIST_PATH}"

/usr/libexec/PlistBuddy -c "Delete :UTImportedTypeDeclarations" "${PLIST_PATH}" || true
/usr/libexec/PlistBuddy -c "Add :UTImportedTypeDeclarations array" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :UTImportedTypeDeclarations:0 dict" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :UTImportedTypeDeclarations:0:UTTypeIdentifier string org.bittorrent.torrent" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :UTImportedTypeDeclarations:0:UTTypeDescription string 'BitTorrent File'" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :UTImportedTypeDeclarations:0:UTTypeConformsTo array" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :UTImportedTypeDeclarations:0:UTTypeConformsTo:0 string public.data" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :UTImportedTypeDeclarations:0:UTTypeTagSpecification dict" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :UTImportedTypeDeclarations:0:UTTypeTagSpecification:public.filename-extension array" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :UTImportedTypeDeclarations:0:UTTypeTagSpecification:public.filename-extension:0 string torrent" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :UTImportedTypeDeclarations:0:UTTypeTagSpecification:public.mime-type array" "${PLIST_PATH}"
/usr/libexec/PlistBuddy -c "Add :UTImportedTypeDeclarations:0:UTTypeTagSpecification:public.mime-type:0 string application/x-bittorrent" "${PLIST_PATH}"
touch "${HANDLER_APP_PATH}"

if [ "${UNSIGNED_BUILD}" = true ]; then
    echo "Skipping app signing for local unsigned build."
else
    sign_app_bundle "${HANDLER_APP_PATH}"
fi

echo "Creating DMG staging folder..."
rm -rf "${DMG_STAGING_DIR}"
mkdir -p "${DMG_STAGING_DIR}"
cp -R "${HANDLER_APP_PATH}" "${DMG_STAGING_DIR}/"
DMG_STAGING_DIR_ABS=$(cd "${DMG_STAGING_DIR}" && pwd)
if osascript << EOF
tell application "Finder"
    set destinationFolder to POSIX file "${DMG_STAGING_DIR_ABS}" as alias
    set applicationsFolder to POSIX file "/Applications" as alias
    make new alias file at destinationFolder to applicationsFolder with properties {name:"Applications"}
end tell
EOF
then
    echo "Created Finder alias to Applications."
else
    echo "::warning:: Could not create Finder alias to Applications; falling back to a symlink."
    ln -s /Applications "${DMG_STAGING_DIR}/Applications"
fi

if [ -f "${DMG_STAGING_DIR}/Applications" ] &&
   [ ! -L "${DMG_STAGING_DIR}/Applications" ] &&
   [ -f "${APPLICATIONS_ICON_FILE}" ] &&
   command -v sips >/dev/null 2>&1 &&
   command -v DeRez >/dev/null 2>&1 &&
   command -v Rez >/dev/null 2>&1 &&
   command -v SetFile >/dev/null 2>&1; then
    echo "Applying default Applications folder icon to Applications alias..."
    APPLICATIONS_ICON_COPY="${DMG_STAGING_DIR}/ApplicationsFolderIcon.icns"
    APPLICATIONS_ICON_RSRC="${DMG_STAGING_DIR}/ApplicationsIcon.rsrc"
    cp "${APPLICATIONS_ICON_FILE}" "${APPLICATIONS_ICON_COPY}"
    sips -i "${APPLICATIONS_ICON_COPY}" >/dev/null
    DeRez -only icns "${APPLICATIONS_ICON_COPY}" > "${APPLICATIONS_ICON_RSRC}"
    Rez -append "${APPLICATIONS_ICON_RSRC}" -o "${DMG_STAGING_DIR}/Applications"
    SetFile -a C "${DMG_STAGING_DIR}/Applications"
    rm -f "${APPLICATIONS_ICON_COPY}" "${APPLICATIONS_ICON_RSRC}"
else
    echo "::warning:: Could not apply the default Applications folder icon to the Applications alias."
fi

CREATE_DMG_BACKGROUND_ARGS=()
if command -v sips >/dev/null 2>&1; then
    echo "Preparing DMG background: ${DMG_BACKGROUND_PATH}"
    sips \
      -z "${DMG_WINDOW_HEIGHT}" "${DMG_WINDOW_WIDTH}" \
      "${DMG_BACKGROUND_SOURCE}" \
      --out "${DMG_BACKGROUND_PATH}" >/dev/null
    CREATE_DMG_BACKGROUND_ARGS=(--background "${DMG_BACKGROUND_PATH}")
else
    echo "::warning:: sips is unavailable; using the source DMG background without resizing."
    CREATE_DMG_BACKGROUND_ARGS=(--background "${DMG_BACKGROUND_SOURCE}")
fi

mkdir -p "${DMG_OUTPUT_DIR}"
rm -f "${DMG_OUTPUT_PATH}" "${DMG_TEMP_PATH}" "${DMG_LAYOUT_PATH}"
rm -rf "${DMG_MOUNT_DIR}"

if command -v create-dmg >/dev/null 2>&1; then
    echo "Creating drag-to-Applications DMG with create-dmg..."
    CREATE_DMG_OUTPUT_PATH="${DMG_OUTPUT_PATH}"
    if [ "${UNSIGNED_BUILD}" = false ]; then
        CREATE_DMG_OUTPUT_PATH="${DMG_LAYOUT_PATH}"
    fi

    create-dmg \
      --volname "${APP_NAME}" \
      --volicon "${ICON_FILE_PATH}" \
      "${CREATE_DMG_BACKGROUND_ARGS[@]}" \
      --window-pos 200 120 \
      --window-size "${DMG_WINDOW_WIDTH}" "${DMG_WINDOW_HEIGHT}" \
      --icon-size 112 \
      --text-size 14 \
      --icon "${HANDLER_APP_NAME}.app" 160 225 \
      --icon "Applications" 610 225 \
      --no-internet-enable \
      --format UDZO \
      "${CREATE_DMG_OUTPUT_PATH}" \
      "${DMG_STAGING_DIR}"

    if [ "${UNSIGNED_BUILD}" = false ]; then
        echo "Converting layout DMG to read-write image..."
        hdiutil convert \
          "${DMG_LAYOUT_PATH}" \
          -format UDRW \
          -ov \
          -o "${DMG_TEMP_PATH}"

        sign_app_inside_readwrite_dmg "${DMG_TEMP_PATH}" "${DMG_MOUNT_DIR}"

        echo "Compressing signed read-write DMG at ${DMG_OUTPUT_PATH}..."
        hdiutil convert \
          "${DMG_TEMP_PATH}" \
          -format UDZO \
          -imagekey zlib-level=9 \
          -ov \
          -o "${DMG_OUTPUT_PATH}"
        rm -f "${DMG_LAYOUT_PATH}" "${DMG_TEMP_PATH}"
    fi
else
    echo "create-dmg is unavailable; falling back to a plain hdiutil DMG."
    if [ ! -e "${DMG_STAGING_DIR}/Applications" ]; then
        ln -s /Applications "${DMG_STAGING_DIR}/Applications"
    fi
    cp "${ICON_FILE_PATH}" "${DMG_STAGING_DIR}/.VolumeIcon.icns"

    echo "Creating compressed DMG at ${DMG_OUTPUT_PATH}..."
    hdiutil create \
      -volname "${APP_NAME}" \
      -srcfolder "${DMG_STAGING_DIR}" \
      -ov \
      -format UDZO \
      "${DMG_OUTPUT_PATH}"
fi

if [ "${UNSIGNED_BUILD}" = true ]; then
    echo "Skipping DMG signing for local unsigned build."
else
    echo "Signing DMG with Developer ID Application certificate..."
    codesign -s "${APP_CERT_NAME}" \
      -v --force \
      "${CODESIGN_TIMESTAMP_ARGS[@]}" \
      "${DMG_OUTPUT_PATH}"
    verify_code_signature "${DMG_OUTPUT_PATH}"
    verify_dmg_app_signature "${DMG_OUTPUT_PATH}" "${DMG_MOUNT_DIR}"
fi

rm -rf "${HANDLER_STAGING_DIR}"
rm -rf "${DMG_STAGING_DIR}"
rm -rf "${DMG_MOUNT_DIR}"
rm -rf "${UNIVERSAL_STAGING_DIR}"
rm -f "${DMG_LAYOUT_PATH}"
rm -f "${DMG_TEMP_PATH}"
rm -f "${ENTITLEMENTS_PATH}"
rm -f "${DMG_BACKGROUND_PATH}"

echo ""
echo "DMG creation complete at: ${DMG_OUTPUT_PATH}"
echo "--------------------------------------------------------"
echo "DMG_PATH=${DMG_OUTPUT_PATH}"
echo "DMG_NAME=${DMG_NAME}"
