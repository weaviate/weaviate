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

// Package changelog provides an append-only, per-op change capture log used by
// replica movement to deterministically replay writes that land on the source
// after the file snapshot is taken.
package changelog

import "errors"

var (
	ErrLogFinalized   = errors.New("changelog: log is finalized")
	ErrLogDeactivated = errors.New("changelog: log is deactivated")
	ErrCRCMismatch    = errors.New("changelog: frame CRC mismatch")
)
