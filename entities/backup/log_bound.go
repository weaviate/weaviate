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

package backup

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	// logErrMaxLines is how many of a joined error's lines a log line
	// keeps. Enough to recognize the failure, few enough that the line
	// stays readable.
	logErrMaxLines = 5
	// logErrMaxBytes caps the result even if the kept lines are long.
	logErrMaxBytes = 4096
)

// ErrorForLog renders err for a log line with a size that does not grow
// with the number of collections: an admission refusal carries one line
// per blocked collection, so a wide pass produces an error that gets
// dropped or truncated somewhere the operator doesn't control.
//
// Keeps the first few lines plus a count of what was dropped. Log only:
// the HTTP response keeps every line for the caller that asked for it.
func ErrorForLog(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	bounded := TextForLog(msg)
	if bounded == msg {
		return err
	}
	return errors.New(bounded)
}

// TextForLog is [ErrorForLog] for a message that has already been
// flattened to a string, which is how a participant's refusal reaches
// the coordinator.
func TextForLog(msg string) string {
	lines := strings.Split(msg, "\n")
	total := len(lines)
	if total > logErrMaxLines {
		lines = lines[:logErrMaxLines]
	}
	out := strings.Join(lines, "\n")
	if total > logErrMaxLines {
		out += fmt.Sprintf("\n... and %d more of %d (the full list is in the API response)",
			total-logErrMaxLines, total)
	}
	if len(out) > logErrMaxBytes {
		cut := logErrMaxBytes
		for cut > 0 && !utf8.RuneStart(out[cut]) {
			cut--
		}
		out = out[:cut] + fmt.Sprintf("... (truncated, %d bytes total)", len(msg))
	}
	return out
}
