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

// rescoreLimit and rescoreFactor are shared by every hnsw quantizer and decide
// how many candidates a search re-ranks with their uncompressed vectors: of the
// candidates the graph search returns (at most ef), it reads back
// min(ef, max(k, rescoreLimit, ceil(k * rescoreFactor))). rescoreLimit is the
// minimum, which keeps small-k searches on their tuned depth; rescoreFactor
// scales the depth with k, so a large k keeps headroom over the k it returns
// without ef turning the whole candidate list into disk reads. Every candidate
// is one random read of the objects bucket.
const (
	// RescoreLimitAuto is the stored default: Weaviate picks the quantizer's
	// own default, so an index created without the key follows later tuning
	// of that default instead of freezing a number into its schema.
	RescoreLimitAuto = -1
	// RescoreLimitDisabled switches rescoring off: results carry the
	// compressed distances.
	RescoreLimitDisabled = 0
	// RescoreFactorAuto is the stored default of rescoreFactor and resolves
	// to the quantizer's own factor, like RescoreLimitAuto. 0 resolves the
	// same way.
	RescoreFactorAuto = common.RescoreFactorAuto
)

// RescorePolicy is the resolved rescore setting of one quantizer. Limit 0
// disables rescoring for hnsw.
type RescorePolicy = common.RescorePolicy

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

// RescorePolicy resolves both PQ rescore settings.
func (c PQConfig) RescorePolicy() RescorePolicy {
	return RescorePolicy{
		Limit:  c.EffectiveRescoreLimit(),
		Factor: common.ResolveRescoreFactor(c.RescoreFactor, DefaultPQRescoreFactor),
	}
}

// RescorePolicy resolves both BQ rescore settings.
func (c BQConfig) RescorePolicy() RescorePolicy {
	return RescorePolicy{
		Limit:  c.EffectiveRescoreLimit(),
		Factor: common.ResolveRescoreFactor(c.RescoreFactor, DefaultBQRescoreFactor),
	}
}

// RescorePolicy resolves both SQ rescore settings.
func (c SQConfig) RescorePolicy() RescorePolicy {
	return RescorePolicy{
		Limit:  c.EffectiveRescoreLimit(),
		Factor: common.ResolveRescoreFactor(c.RescoreFactor, DefaultSQRescoreFactor),
	}
}

// RescorePolicy resolves both RQ rescore settings; like the limit, the
// automatic factor depends on the bit width.
func (c RQConfig) RescorePolicy() RescorePolicy {
	auto := DefaultRQRescoreFactor
	if c.Bits == 1 {
		auto = DefaultBRQRescoreFactor
	}
	return RescorePolicy{
		Limit:  c.EffectiveRescoreLimit(),
		Factor: common.ResolveRescoreFactor(c.RescoreFactor, auto),
	}
}
