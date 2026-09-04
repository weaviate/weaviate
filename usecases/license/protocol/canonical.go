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

package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Canonical returns the canonical JSON encoding of v: object keys sorted,
// no insignificant whitespace, HTML characters not escaped. Signatures in
// this protocol are always computed over canonical bytes so that both sides
// can re-derive them from parsed values.
func Canonical(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("license: canonical marshal: %w", err)
	}
	// Round-trip through a generic value so map keys are emitted sorted.
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, fmt.Errorf("license: canonical unmarshal: %w", err)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(generic); err != nil {
		return nil, fmt.Errorf("license: canonical encode: %w", err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
