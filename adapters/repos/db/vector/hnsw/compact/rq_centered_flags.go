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

package compact

import (
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

// Centered RQ carries its code-layout decisions in a single flags byte that
// follows the record type in both persistence formats (AddRQCentered in the
// WAL, SnapshotCompressionTypeRQCentered in a snapshot). Every future lever
// that changes the stored code length gets one bit here instead of its own
// record type, which would otherwise need one constant per combination of
// levers. No bit is defined yet — the centered layout is currently fixed —
// so the byte is written as zero and any set bit is rejected.
//
// The byte sits immediately after the type, ahead of the RQ payload, so a
// future flag can govern payload that follows it.
//
// Loudness on version skew is preserved by the mask below rather than by the
// record type: released binaries (<= v1.39) reject the centered type itself
// and never reach the flags, and a binary that meets a flag it does not know
// refuses the record instead of restoring a quantizer that would read every
// code at the wrong length.

// rqCenteredFlagsKnown is the mask of every flag this binary understands.
const rqCenteredFlagsKnown = 0

// encodeRQCenteredFlags packs the layout decisions of a centered RQ fit into
// the flags byte.
func encodeRQCenteredFlags(data *compression.RQData) byte {
	return 0
}

// applyRQCenteredFlags unpacks the flags byte onto data, rejecting any flag
// this binary does not implement: the unknown bit would change the code
// length, so restoring would silently misparse every vector.
func applyRQCenteredFlags(data *compression.RQData, flags byte) error {
	if unknown := flags &^ rqCenteredFlagsKnown; unknown != 0 {
		return errors.Errorf("centered RQ flags 0x%02x contain unknown bits 0x%02x, "+
			"the index was written by a newer version", flags, unknown)
	}
	return nil
}
