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

package clusterprobe

import (
	"strconv"
	"unicode/utf8"
)

const (
	loggableLimit            = 128
	loggableTruncationMarker = "…(truncated)"
)

// Loggable renders a string a peer controls safe for a log line: quoted so a
// newline cannot forge a second line, capped so the peer cannot choose its size.
func Loggable(s string) string {
	if len(s) > loggableLimit {
		cut := loggableLimit
		// Back up to a rune boundary, or the kept part ends in half a rune.
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut] + loggableTruncationMarker
	}
	return strconv.Quote(s)
}
