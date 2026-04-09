// Package examples embeds static HTML demos for llmpipe (served by cmd/voicebot).
package examples

import "embed"

// FS is the root file system for demo assets (e.g. /demo/voicebot-client.html).
//
//go:embed *.html
var FS embed.FS
