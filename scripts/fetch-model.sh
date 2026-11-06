#!/bin/sh
# Download one file and require an exact SHA256.
# Usage: fetch-model.sh URL DEST SHA256
set -eu
url=$1
dest=$2
want=$3
mkdir -p "$(dirname "$dest")"
if [ -f "$dest" ]; then
	got=$(sha256sum "$dest" | awk '{print $1}')
	if [ "$got" = "$want" ]; then
		exit 0
	fi
	echo "checksum mismatch for $dest, downloading again" >&2
fi
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
curl -fsSL -o "$tmp" "$url"
got=$(sha256sum "$tmp" | awk '{print $1}')
if [ "$got" != "$want" ]; then
	echo "sha256 mismatch for $url" >&2
	echo " got  $got" >&2
	echo " want $want" >&2
	exit 1
fi
mv "$tmp" "$dest"
trap - EXIT
