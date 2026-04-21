// Regenerate transcriptions/language_constants.go from Pipecat language.py.
package main

import (
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func main() {
	wd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// go generate runs in the package dir (e.g. llmpipe/transcriptions).
	root := filepath.Clean(filepath.Join(wd, "..", ".."))
	src := filepath.Join(root, "pipecat", "src", "pipecat", "transcriptions", "language.py")
	out := filepath.Join(wd, "language_constants.go")

	textBytes, err := os.ReadFile(src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read %s: %v\n", src, err)
		os.Exit(1)
	}
	text := string(textBytes)

	re := regexp.MustCompile(`^\s+([A-Z][A-Z0-9_]*)\s*=\s*"([^"]*)"\s*$`)
	var pairs [][2]string
	for _, line := range strings.Split(text, "\n") {
		line = strings.Split(line, "#")[0]
		if m := re.FindStringSubmatch(line); m != nil {
			pairs = append(pairs, [2]string{m[1], m[2]})
		}
	}

	var b strings.Builder
	b.WriteString("// Code generated from Pipecat pipecat/transcriptions/language.py; DO NOT EDIT.\n\n")
	b.WriteString("package transcriptions\n\nconst (\n")
	for _, p := range pairs {
		fmt.Fprintf(&b, "\t%s Language = %q\n", p[0], p[1])
	}
	b.WriteString(")\n")

	formatted, err := format.Source([]byte(b.String()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "format generated source: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(out, formatted, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", out, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %d constants to %s\n", len(pairs), out)
}
