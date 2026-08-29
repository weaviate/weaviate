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

// RQ4c uses a flags byte for backwards compatible code-layout.
// The byte sits immediately after the type, ahead of the RQ payload, so a
// future flag can govern payload that follows it.

const rqCenteredFlagsKnown = 0

func encodeRQCenteredFlags(data *compression.RQData) byte {
	return 0
}

func applyRQCenteredFlags(data *compression.RQData, flags byte) error {
	if unknown := flags &^ rqCenteredFlagsKnown; unknown != 0 {
		return errors.Errorf("centered RQ flags 0x%02x contain unknown bits 0x%02x, "+
			"the index was written by a newer version", flags, unknown)
	}
	return nil
}
