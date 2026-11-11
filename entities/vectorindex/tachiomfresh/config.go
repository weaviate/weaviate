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

// Package tachiomfresh holds the user configuration of the TachiomFresh
// vector index type, a token-level inverted-file (IVF) index for
// multi-vectors. It follows the shape of entities/vectorindex/hfresh.
//
// Two limits come from the posting entry, which stores a token as a doc id
// (u32) and a position (u16): the index holds documents with doc ids up to
// 4,294,967,295, and documents of at most 65,536 tokens. An insert beyond
// either is refused, whether the index stages or is built.
package tachiomfresh

import (
	"errors"
	"fmt"
	"math"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	vectorIndexCommon "github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	// DefaultMaxPostingSize is the number of tokens a posting holds before
	// it is split. Among the sizes the tests tried, 64 gave the best
	// balance of recall and speed.
	DefaultMaxPostingSize = 64
	// MaxPostingSizeFloor is the smallest maxPostingSize accepted. The merge
	// threshold is max(maxPostingSize/3, 3) and must stay below the split
	// threshold. 4 is the first value that satisfies this.
	MaxPostingSizeFloor = 4
	// MaximumAllowedMaxPostingSize is the largest maxPostingSize a schema
	// write accepts. It only stops absurd values, far above any useful
	// posting size. See ValidateUpperBounds.
	MaximumAllowedMaxPostingSize = 100_000
	// MaximumAllowedEFConstruction is the largest efConstruction a schema
	// write accepts. It only stops absurd values: the centroid graph never
	// needs more. See ValidateUpperBounds.
	MaximumAllowedEFConstruction = 1024
	// MaximumAllowedKCentroids is the largest kCentroids a schema write
	// accepts. kCentroids is the k of the centroid graph's search, whose
	// queues are sized from k. See ValidateUpperBounds.
	MaximumAllowedKCentroids = 4096
	// MaximumAllowedKDocsToScore is the largest kDocsToScore a schema write
	// accepts: every candidate is read and scored exactly. See
	// ValidateUpperBounds.
	MaximumAllowedKDocsToScore = 20_000
	// DefaultKCentroids is the number of centroids the search of the
	// centroid graph returns per query token (TACHIOM's n_probe, stage 1
	// of the search).
	DefaultKCentroids = 32
	// DefaultKDocsToScore is the number of candidate documents the exact
	// MaxSim scoring scores (stage 2 of the search).
	DefaultKDocsToScore = 500
	// DefaultDistance is the only distance the index accepts. The metric of a
	// multi-vector index is MaxSim, whose single-vector metric is the dot
	// product.
	DefaultDistance = vectorIndexCommon.DistanceDot
	// DefaultEFConstruction is the efConstruction of the centroid HNSW. It
	// is hfresh's value, 64.
	DefaultEFConstruction = 64
)

// UserConfig is the user-settable configuration of a TachiomFresh index.
// The index is multi-vector only. Multivector is kept because the other
// index types have the same field, and it must be enabled.
type UserConfig struct {
	MaxPostingSize uint32                 `json:"maxPostingSize"`
	KCentroids     uint32                 `json:"kCentroids"`
	KDocsToScore   uint32                 `json:"kDocsToScore"`
	EFConstruction int                    `json:"efConstruction"`
	Distance       string                 `json:"distance"`
	Multivector    hnsw.MultivectorConfig `json:"multivector"`
}

// IndexType returns the type of the underlying vector index. It implements
// the schema.VectorIndexConfig interface.
func (u UserConfig) IndexType() string {
	return "tachiomfresh"
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

// IsMultiVector is always true. The index holds only multi-vectors, and
// validate refuses a config with multivector disabled.
func (u UserConfig) IsMultiVector() bool {
	return true
}

// SetDefaults sets the user-settable fields of the config to their defaults.
func (u *UserConfig) SetDefaults() {
	u.MaxPostingSize = DefaultMaxPostingSize
	u.KCentroids = DefaultKCentroids
	u.KDocsToScore = DefaultKDocsToScore
	u.EFConstruction = DefaultEFConstruction
	u.Distance = DefaultDistance
	u.Multivector.Enabled = true
	u.Multivector.Aggregation = hnsw.DefaultMultivectorAggregation
	u.Multivector.MuveraConfig.Enabled = false
}

func NewDefaultUserConfig() UserConfig {
	var uc UserConfig
	uc.SetDefaults()
	return uc
}

func (u *UserConfig) validate() error {
	var errs []error

	if u.Distance != DefaultDistance {
		errs = append(errs, fmt.Errorf(
			"unsupported distance type '%s', TachiomFresh only supports 'dot' for the distance metric",
			u.Distance,
		))
	}

	if u.MaxPostingSize < MaxPostingSizeFloor {
		errs = append(errs, fmt.Errorf(
			"maxPostingSize is '%d' but must be at least %d",
			u.MaxPostingSize,
			MaxPostingSizeFloor,
		))
	}

	if u.KCentroids == 0 {
		errs = append(errs, errors.New("kCentroids must be greater than 0"))
	}

	if u.KDocsToScore == 0 {
		errs = append(errs, errors.New("kDocsToScore must be greater than 0"))
	}

	if u.EFConstruction <= 0 {
		errs = append(errs, errors.New("efConstruction must be greater than 0"))
	}

	if !u.Multivector.Enabled {
		errs = append(errs, errors.New("TachiomFresh is a multi-vector index: multivector must be enabled"))
	}

	// the index scores by MaxSim only (stage 3)
	if u.Multivector.Aggregation != hnsw.MultivectorAggregationMaxSim {
		errs = append(errs, fmt.Errorf(
			"unsupported multivector aggregation '%s', TachiomFresh only supports '%s'",
			u.Multivector.Aggregation, hnsw.MultivectorAggregationMaxSim,
		))
	}

	if u.Multivector.MuveraConfig.Enabled {
		errs = append(errs, errors.New("TachiomFresh indexes tokens directly and does not support muvera"))
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid tachiomfresh config: %w", errors.Join(errs...))
	}

	return nil
}

// parseAndValidateQuantization refuses every quantization key. The index
// keeps float32 tokens in the objects bucket and has no codes of its own.
func parseAndValidateQuantization(ucMap map[string]interface{}) error {
	for _, key := range []string{"pq", "sq", "bq", "rq"} {
		if _, ok := ucMap[key]; ok {
			return fmt.Errorf("%s is not supported for tachiomfresh index (no quantization)", key)
		}
	}
	return nil
}

// ParseAndValidateConfig parses the config from an untyped input value. The
// API leaves the value untyped so that the index type can be exchanged.
// isMultiVector is accepted because the registry calls this signature; the
// index is multi-vector whether or not the vectorizer says so.
func ParseAndValidateConfig(input interface{}, isMultiVector bool) (schemaConfig.VectorIndexConfig, error) {
	uc := UserConfig{}
	uc.SetDefaults()

	if input == nil {
		return uc, nil
	}

	asMap, ok := input.(map[string]interface{})
	if !ok || asMap == nil {
		return uc, fmt.Errorf("input must be a non-nil map")
	}

	if err := parseAndValidateQuantization(asMap); err != nil {
		return uc, err
	}

	if err := optionalUint32FromMap(asMap, "maxPostingSize", func(v uint32) {
		uc.MaxPostingSize = v
	}); err != nil {
		return uc, err
	}

	if err := optionalUint32FromMap(asMap, "kCentroids", func(v uint32) {
		uc.KCentroids = v
	}); err != nil {
		return uc, err
	}

	if err := optionalUint32FromMap(asMap, "kDocsToScore", func(v uint32) {
		uc.KDocsToScore = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "efConstruction", func(v int) {
		uc.EFConstruction = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return uc, err
	}

	if err := parseMultivectorConfig(asMap, &uc.Multivector); err != nil {
		return uc, err
	}

	return uc, uc.validate()
}

// optionalUint32FromMap reads an integer field that the config holds as a
// uint32. A value the type cannot hold is an error: the cast would turn -1
// into 4294967295 and 4294967301 into 5. A number above the int64 range
// arrives here as an out-of-range value too.
func optionalUint32FromMap(in map[string]interface{}, name string, setFn func(v uint32)) error {
	var rangeErr error
	err := vectorIndexCommon.OptionalIntFromMap(in, name, func(v int) {
		if v < 0 || v > math.MaxUint32 {
			rangeErr = fmt.Errorf("invalid tachiomfresh config: %s is '%d' but must be between 0 and %d",
				name, v, uint32(math.MaxUint32))
			return
		}
		setFn(uint32(v))
	})
	if err != nil {
		return err
	}
	return rangeErr
}

// ValidateUpperBounds enforces the maxPostingSize, efConstruction,
// kCentroids and kDocsToScore upper bounds. It runs on schema create and update only, never when a persisted
// schema is parsed, so a stored class with a larger value does not block a
// node from starting.
func ValidateUpperBounds(uc UserConfig) error {
	var errs []error
	if uc.MaxPostingSize > MaximumAllowedMaxPostingSize {
		errs = append(errs, fmt.Errorf("maxPostingSize is %d but must be at most %d",
			uc.MaxPostingSize, MaximumAllowedMaxPostingSize))
	}
	if uc.EFConstruction > MaximumAllowedEFConstruction {
		errs = append(errs, fmt.Errorf("efConstruction is %d but must be at most %d",
			uc.EFConstruction, MaximumAllowedEFConstruction))
	}
	if uc.KCentroids > MaximumAllowedKCentroids {
		errs = append(errs, fmt.Errorf("kCentroids is %d but must be at most %d",
			uc.KCentroids, MaximumAllowedKCentroids))
	}
	if uc.KDocsToScore > MaximumAllowedKDocsToScore {
		errs = append(errs, fmt.Errorf("kDocsToScore is %d but must be at most %d",
			uc.KDocsToScore, MaximumAllowedKDocsToScore))
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid tachiomfresh config: %w", errors.Join(errs...))
	}
	return nil
}

// parseMultivectorConfig reads the multivector block as hfresh's parser
// does, without the muvera parameters. Only muvera's enabled flag is read,
// so that validate can refuse an attempt to turn muvera on. Without the
// flag the attempt would be ignored.
func parseMultivectorConfig(in map[string]interface{}, multivector *hnsw.MultivectorConfig) error {
	multivectorValue, ok := in["multivector"]
	if !ok {
		return nil
	}

	multivectorMap, ok := multivectorValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := vectorIndexCommon.OptionalBoolFromMap(multivectorMap, "enabled", func(v bool) {
		multivector.Enabled = v
	}); err != nil {
		return err
	}

	if err := vectorIndexCommon.OptionalStringFromMap(multivectorMap, "aggregation", func(v string) {
		multivector.Aggregation = v
	}); err != nil {
		return err
	}

	muveraValue, ok := multivectorMap["muvera"]
	if !ok {
		return nil
	}

	muveraMap, ok := muveraValue.(map[string]interface{})
	if !ok {
		return nil
	}

	return vectorIndexCommon.OptionalBoolFromMap(muveraMap, "enabled", func(v bool) {
		multivector.MuveraConfig.Enabled = v
	})
}
