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

package conv

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"slices"
	"strings"
	"unicode"
)

// maxStorableValueLength bounds a user-supplied value in a policy row. It is far
// above any OIDC subject (at most 255 characters by spec) or group name, and
// keeps every row well below the line length the policy file loader reads.
const maxStorableValueLength = 4096

// ValidateStorableValue rejects user input that would corrupt the policy file:
// ',' and '"' are the CSV separator and quote, and '\n' splits the row. It also
// refuses the other control characters. ValidateStorableRow accepts those, so
// rows already stored with one, such as a tab, keep restoring.
func ValidateStorableValue(value string) error {
	if len(value) > maxStorableValueLength {
		return fmt.Errorf("must not be longer than %d bytes", maxStorableValueLength)
	}
	for _, r := range value {
		if r == ',' || r == '"' || unicode.IsControl(r) {
			return fmt.Errorf("must not contain %q", r)
		}
	}
	return nil
}

// ValidateStorableRow reports whether a policy row, ptype first, reads back
// unchanged from the policy file. casbin's file adapter joins the fields with
// ", " and does not quote them. On load it splits the file on '\n', trims each
// line and parses it with encoding/csv. A row that does not read back unchanged
// either fails every later load, so the node can't start, or loads as other
// rows: a '\n' starts a row of the writer's choosing, and casbin ignores extra
// fields on a 'g' row rather than rejecting it.
func ValidateStorableRow(row ...string) error {
	line := strings.Join(row, ", ")
	if strings.Contains(line, "\n") {
		return fmt.Errorf("policy row %q cannot be stored: it contains a line break", line)
	}
	// The loader's bufio.Scanner fails on a line that fills its whole buffer.
	if len(line) >= bufio.MaxScanTokenSize {
		return fmt.Errorf("policy row cannot be stored: it is %d bytes long, the limit is %d", len(line), bufio.MaxScanTokenSize-1)
	}
	r := csv.NewReader(strings.NewReader(strings.TrimSpace(line)))
	r.Comma = ','
	r.Comment = '#'
	r.TrimLeadingSpace = true
	got, err := r.Read()
	if err != nil {
		return fmt.Errorf("policy row %q cannot be stored: %w", line, err)
	}
	if !slices.Equal(got, row) {
		return fmt.Errorf("policy row %q cannot be stored: it reads back as %q", line, got)
	}
	return nil
}
