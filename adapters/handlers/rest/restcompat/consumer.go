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

package restcompat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
)

// NewStrictJSONConsumer wraps the default JSON consumer to reject JSON payloads
// containing lone UTF-16 surrogates (\uD800-\uDFFF) that are not part of a
// valid surrogate pair. Go's encoding/json silently normalizes these to U+FFFD
// instead of returning an error, so we must scan the raw bytes before
// unmarshaling to enforce RFC 8259 compliance.
func NewStrictJSONConsumer() runtime.Consumer {
	return runtime.ConsumerFunc(func(r io.Reader, target interface{}) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		if err := rejectLoneSurrogates(data); err != nil {
			return err
		}

		return json.Unmarshal(data, target)
	})
}

// rejectLoneSurrogates scans raw JSON bytes for Unicode escape sequences that
// encode lone UTF-16 surrogates. A valid surrogate pair is a high surrogate
// (\uD800-\uDBFF) immediately followed by a low surrogate (\uDC00-\uDFFF).
// Any surrogate that appears without its pair is rejected.
//
// The function uses a fast-path check: if the raw bytes do not contain the
// substring `\u`, no scanning is performed.
func rejectLoneSurrogates(data []byte) error {
	// Fast path: no unicode escapes at all.
	if !bytes.Contains(data, []byte(`\u`)) {
		return nil
	}

	n := len(data)
	for i := 0; i < n; i++ {
		if data[i] != '\\' {
			continue
		}

		slashes := countBackslashes(data, i)
		// Advance past all but the last backslash.
		i += slashes - 1

		// Even number of backslashes means they cancel out (\\\\) and the
		// character after them is NOT escaped.
		if slashes%2 == 0 {
			continue
		}

		if err, skip := checkUnicodeEscape(data, i); err != nil {
			return err
		} else if skip > 0 {
			i += skip
		}
	}

	return nil
}

// countBackslashes returns the number of consecutive backslashes starting at index i.
func countBackslashes(data []byte, start int) int {
	count := 0
	for i := start; i < len(data) && data[i] == '\\'; i++ {
		count++
	}
	return count
}

// checkUnicodeEscape verifies a unicode escape sequence starting at index i.
// It returns an error if a lone surrogate is found, or the number of additional
// bytes to skip if a valid surrogate pair is found.
func checkUnicodeEscape(data []byte, i int) (error, int) {
	if i+5 >= len(data) || data[i+1] != 'u' {
		return nil, 0
	}

	codepoint, ok := parseHex4(data[i+2 : i+6])
	if !ok {
		return nil, 0
	}

	if isHighSurrogate(codepoint) {
		// A high surrogate MUST be followed by \uDC00-\uDFFF.
		if hasValidLowSurrogate(data, i+6) {
			// Valid pair — skip past both escapes.
			return nil, 11
		}
		return fmt.Errorf("invalid JSON: lone high UTF-16 surrogate \\u%04X at byte offset %d", codepoint, i), 0
	}

	if isLowSurrogate(codepoint) {
		return fmt.Errorf("invalid JSON: lone low UTF-16 surrogate \\u%04X at byte offset %d", codepoint, i), 0
	}

	return nil, 0
}

// hasValidLowSurrogate checks if the bytes starting at index start encode a valid low surrogate.
func hasValidLowSurrogate(data []byte, start int) bool {
	if start+5 < len(data) && data[start] == '\\' && data[start+1] == 'u' {
		low, ok := parseHex4(data[start+2 : start+6])
		return ok && isLowSurrogate(low)
	}
	return false
}

// isHighSurrogate reports whether the code unit is a UTF-16 high surrogate
// (U+D800 through U+DBFF).
func isHighSurrogate(c uint16) bool {
	return c >= 0xD800 && c <= 0xDBFF
}

// isLowSurrogate reports whether the code unit is a UTF-16 low surrogate
// (U+DC00 through U+DFFF).
func isLowSurrogate(c uint16) bool {
	return c >= 0xDC00 && c <= 0xDFFF
}

// parseHex4 parses exactly 4 hex ASCII bytes into a uint16.
// Returns false if any byte is not a valid hex digit.
func parseHex4(b []byte) (uint16, bool) {
	var v uint16
	for _, c := range b {
		v <<= 4
		switch {
		case c >= '0' && c <= '9':
			v |= uint16(c - '0')
		case c >= 'a' && c <= 'f':
			v |= uint16(c-'a') + 10
		case c >= 'A' && c <= 'F':
			v |= uint16(c-'A') + 10
		default:
			return 0, false
		}
	}
	return v, true
}
