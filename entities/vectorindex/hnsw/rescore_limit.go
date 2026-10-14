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

package hnsw

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

// rescoreLimit is shared by every hnsw quantizer and bounds how many
// candidates a search re-ranks with their uncompressed vectors: of the
// candidates the graph search returns, min(ef, max(k, rescoreLimit)) are read
// back. Every candidate is one random read of the objects bucket, so the bound
// is what keeps a large k from turning the whole ef into disk reads.
const (
	// RescoreLimitAuto is the stored default: Weaviate picks the quantizer's
	// own default, so an index created without the key follows later tuning
	// of that default instead of freezing a number into its schema.
	RescoreLimitAuto = -1
	// RescoreLimitDisabled switches rescoring off: results carry the
	// compressed distances.
	RescoreLimitDisabled = 0
)

// parseRescoreLimit reads an optional rescoreLimit key. -1 is automatic, 0
// disables rescoring, anything further below -1 is rejected.
func parseRescoreLimit(in map[string]interface{}, quantizer string, setFn func(v int)) error {
	var parsed *int
	if err := common.OptionalIntFromMap(in, "rescoreLimit", func(v int) {
		parsed = &v
	}); err != nil {
		return err
	}
	if parsed == nil {
		return nil
	}
	if *parsed < RescoreLimitAuto {
		return fmt.Errorf("%s rescoreLimit must be %d (automatic), %d (disabled) or positive, got %d",
			quantizer, RescoreLimitAuto, RescoreLimitDisabled, *parsed)
	}
	setFn(*parsed)
	return nil
}

func resolveRescoreLimit(configured, auto int) int {
	if configured < 0 {
		return auto
	}
	return configured
}

// EffectiveRescoreLimit resolves RescoreLimitAuto to the PQ default.
func (c PQConfig) EffectiveRescoreLimit() int {
	return resolveRescoreLimit(c.RescoreLimit, DefaultPQRescoreLimit)
}

// EffectiveRescoreLimit resolves RescoreLimitAuto to the BQ default.
func (c BQConfig) EffectiveRescoreLimit() int {
	return resolveRescoreLimit(c.RescoreLimit, DefaultBQRescoreLimit)
}

// EffectiveRescoreLimit resolves RescoreLimitAuto to the SQ default.
func (c SQConfig) EffectiveRescoreLimit() int {
	return resolveRescoreLimit(c.RescoreLimit, DefaultSQRescoreLimit)
}

// EffectiveRescoreLimit resolves RescoreLimitAuto to the RQ default, which
// depends on the bit width: 1-bit codes lose far more precision than 4- or
// 8-bit ones and need a deeper rescore.
func (c RQConfig) EffectiveRescoreLimit() int {
	auto := DefaultRQRescoreLimit
	if c.Bits == 1 {
		auto = DefaultBRQRescoreLimit
	}
	return resolveRescoreLimit(c.RescoreLimit, auto)
}
