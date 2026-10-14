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
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// maxStorableValueLength caps OIDC user IDs and group names, the only values in a
// policy row with no limit of their own. 256 fits any OIDC subject (255 by spec)
// and matches maxTargetLength for user and group patterns in a permission.
const maxStorableValueLength = 256

// ValidateStorableValue rejects user input longer than maxStorableValueLength or
// with a character ValidateStorableCharacters refuses.
func ValidateStorableValue(value string) error {
	if len(value) > maxStorableValueLength {
		return fmt.Errorf("must not be longer than %d bytes", maxStorableValueLength)
	}
	return ValidateStorableCharacters(value)
}

// ValidateStorableCharacters rejects ',' and '"', the policy file's field
// separator and quote, and any control character, such as the '\n' ending a row.
// ValidateStorableRow accepts a tab, so rows already stored with one still load.
// It also rejects invalid UTF-8, which a path parameter can carry and the RAFT
// log would store as U+FFFD.
func ValidateStorableCharacters(value string) error {
	if !utf8.ValidString(value) {
		return errors.New("must be valid UTF-8")
	}
	for _, r := range value {
		if r == ',' || r == '"' || unicode.IsControl(r) {
			return fmt.Errorf("must not contain %q", r)
		}
	}
	return nil
}

// ValidateStorableRow reports whether a policy row, ptype first, survives a save
// and load through casbin's file adapter, which writes fields unquoted. A row
// that does not can keep the node from starting, or read back as an admin grant.
// Each step below repeats one of the adapter's, since casbin accepts any value
// and runs the full load only on a file. The rbac package's
// TestValidateStorableRowMatchesFileAdapter fails if casbin changes a step.
func ValidateStorableRow(row ...string) error {
	// The adapter's SavePolicy writes this line.
	line := strings.Join(row, ", ")
	// The adapter's loadPolicyFile reads the file with a bufio.Scanner, which ends
	// a row at a line break and fails on a line that fills its whole buffer.
	if strings.Contains(line, "\n") {
		return fmt.Errorf("policy row %q cannot be stored: it contains a line break", line)
	}
	if len(line) >= bufio.MaxScanTokenSize {
		return fmt.Errorf("policy row cannot be stored: it is %d bytes long, the limit is %d", len(line), bufio.MaxScanTokenSize-1)
	}
	// loadPolicyFile trims the line. persist.LoadPolicyLine then parses it with
	// these csv settings.
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
