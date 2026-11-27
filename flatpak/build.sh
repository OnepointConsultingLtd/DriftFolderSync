#!/usr/bin/env bash
# Build and install the DriftFolderSync Flatpak for the current user.
#
#   flatpak/build.sh            build + install
#   flatpak/build.sh --bundle   also produce driftfoldersync.flatpak (single-file installer)
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
app_id=io.github.driftfoldersync.DriftFolderSync

cd "$root"
echo "==> Vendoring Rust crates (the Flatpak build itself runs offline)"
cargo vendor --locked --versioned-dirs vendor > /dev/null 2>&1

# Use flatpak-builder from the host if present, otherwise the Flathub app.
if command -v flatpak-builder > /dev/null; then
    fb=(flatpak-builder)
else
    fb=(flatpak run --filesystem="$root" org.flatpak.Builder)
fi

echo "==> Making sure the runtime and Rust SDK extension are installed"
flatpak install --user --noninteractive --or-update flathub \
    org.freedesktop.Platform//26.08 org.freedesktop.Sdk//26.08 \
    org.freedesktop.Sdk.Extension.rust-stable//26.08 > /dev/null

echo "==> Building"
cd "$here"
"${fb[@]}" --user --install --force-clean --disable-rofiles-fuse --repo=repo \
    --state-dir=.flatpak-builder build-dir "$app_id.yml"

if [[ "${1:-}" == "--bundle" ]]; then
    echo "==> Creating single-file bundle driftfoldersync.flatpak"
    flatpak build-bundle repo "$root/driftfoldersync.flatpak" "$app_id" \
        --runtime-repo=https://dl.flathub.org/repo/flathub.flatpakrepo
fi

cat <<EOF

Installed. Try:
  flatpak run $app_id --help
Handy alias:
  alias driftfoldersync='flatpak run $app_id'
EOF
