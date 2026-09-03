//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package banner renders the startup banner and repeats it while the node runs.
package banner

import (
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/build"
)

const (
	// artURL serves the art the repeat banner draws. The file lives in the
	// website repository under static/banner/; a new shape gets a new file.
	// TODO: revert to https://weaviate.io/banner/v1.json before merging;
	// weaviate/weaviate-io#3688 is not deployed yet.
	artURL = "https://raw.githubusercontent.com/weaviate/weaviate-io/banner/startup-art/static/banner/v1.json"

	// defaultInterval is how often the banner is repeated after startup.
	defaultInterval = 24 * time.Hour

	// maxArtLines and maxArtColumns bound what a log line and a terminal carry;
	// maxMessageLines bounds the news printed under the banner.
	maxArtLines     = 10
	maxArtColumns   = 100
	maxMessageLines = 3
)

// landingURL is printed on every start and compiled into every release, so
// the docs site keeps the path stable the way it keeps /e/<id> ids stable.
func landingURL() string {
	return enterrors.DocsBaseURL + "/improve-your-cluster"
}

// embeddedArt is drawn at startup and whenever the art cannot be fetched.
var embeddedArt = []string{
	"WEAVIATE",
}

// render builds the banner message: a frame holding the art, the version,
// and the docs and news entries. Newlines stay in the message; the JSON
// formatter escapes them and log viewers render them back.
func render(art, message []string, docsURL string) string {
	lines := []string{""}
	for _, line := range art {
		lines = append(lines, "    "+line)
	}
	lines = append(lines,
		"",
		"        ⇒ Version: "+build.Version,
		"        ⇒ Docs:    "+printable(docsURL),
	)
	for i, line := range message {
		if i == 0 {
			lines = append(lines, "        ⇒ News:    "+printable(line))
			continue
		}
		lines = append(lines, newsIndent+printable(line))
	}
	lines = append(lines, "")

	// The borders and the fill rows span the widest line, gutter included.
	width := 0
	for _, line := range lines {
		if n := utf8.RuneCountInString(line) + 2; n > width {
			width = n
		}
	}

	var b strings.Builder
	b.WriteString("\n╔" + strings.Repeat("═", width) + "\n")
	b.WriteString("║" + strings.Repeat("░", width) + "\n")
	for _, line := range lines {
		b.WriteString("║░░" + line + "\n")
	}
	b.WriteString("║" + strings.Repeat("░", width) + "\n")
	b.WriteString("╚" + strings.Repeat("═", width) + "\n")
	return b.String()
}

// newsIndent aligns the later news lines under the first one's text.
const newsIndent = "                   "

// printable drops control characters, C0 and C1 alike: the text formatter
// writes the banner verbatim, so a control character in a flag value or the
// fetched art could forge a log line or steer the terminal.
func printable(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || r == '\u2028' || r == '\u2029' {
			return -1
		}
		return r
	}, s)
}
