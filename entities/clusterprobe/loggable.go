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
	// loggableLimit caps how much of an untrusted string Loggable keeps. Every
	// value a probe legitimately carries is far shorter; the cap is there for
	// the one an unauthorized caller or a misrouted proxy sends.
	loggableLimit = 128
	// loggableTruncationMarker ends a value that hit the cap.
	loggableTruncationMarker = "…(truncated)"
)

// Loggable renders a string the other end of a probe controls, i.e. a query
// value a handler was sent or a body a client was answered with, so it is safe
// to put in a log field or an error: quoting escapes the newline that would
// otherwise split one line into two forgeable ones, and the cap stops a
// megabyte of it from being written per request.
func Loggable(s string) string {
	if len(s) > loggableLimit {
		// Cut on a rune boundary so the kept part doesn't end in an escaped half rune.
		cut := loggableLimit
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut] + loggableTruncationMarker
	}
	return strconv.Quote(s)
}
