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

package stopwords

import (
	"github.com/weaviate/weaviate/entities/models"
)

// Provider bundles a collection-level fallback detector together with the
// per-collection cache of user-defined preset detectors. Call sites that
// previously had to thread (fallback, presets) through every constructor and
// invoke resolveStopwordDetector at use time can instead hold a single
// Provider and call provider.Get(prop).
//
// A nil Provider is safe to call: Get always returns nil, which makes call
// sites that wrap the result in a tokenizer.StopwordDetector behave as "no
// stopword filtering".
type Provider struct {
	fallback StopwordDetector
	presets  map[string]*Detector
}

// NewProvider builds a Provider. fallback is the collection-level
// stopwordConfig detector; presets is the cached map of user-defined preset
// detectors keyed by preset name. Either may be nil.
func NewProvider(fallback StopwordDetector, presets map[string]*Detector) *Provider {
	return &Provider{
		fallback: fallback,
		presets:  presets,
	}
}

// Get returns the stopword detector to use for prop:
//   - if the property has no textAnalyzer.stopwordPreset, the collection-level
//     fallback is returned;
//   - if the property names a preset that exists in the user-defined cache,
//     the cached detector is returned (user-defined wins over built-in,
//     matching collection-creation override semantics);
//   - otherwise the named preset is resolved against the built-in registry.
//
// Returns an error only when a referenced preset is neither in the cache nor
// a known built-in. Schema validation should reject this at write time, so
// the error path here is defensive.
func (p *Provider) Get(prop *models.Property) (StopwordDetector, error) {
	if p == nil {
		return nil, nil
	}
	if prop == nil || prop.TextAnalyzer == nil || prop.TextAnalyzer.StopwordPreset == "" {
		return p.fallback, nil
	}
	preset := prop.TextAnalyzer.StopwordPreset
	if d, ok := p.presets[preset]; ok {
		return d, nil
	}
	return NewDetectorFromPreset(preset)
}

// Fallback returns the collection-level detector. Useful for call sites that
// don't have a property in hand (e.g. shard-write deletes that build a
// generic searcher).
func (p *Provider) Fallback() StopwordDetector {
	if p == nil {
		return nil
	}
	return p.fallback
}

// Presets exposes the underlying map for legacy callers that still need to
// pass it through. New code should prefer Get.
func (p *Provider) Presets() map[string]*Detector {
	if p == nil {
		return nil
	}
	return p.presets
}
