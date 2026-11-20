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

// Package postingedge holds the user-facing configuration of the posting+edges
// vector index (adapters/repos/db/vector/postingedge). The name of the index
// type is provisional.
package postingedge

import (
	"errors"
	"fmt"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	vectorIndexCommon "github.com/weaviate/weaviate/entities/vectorindex/common"
)

const (
	// IndexType is the name of the index type in a class.
	IndexType = "postingedge"

	// DefaultEF is the smallest ef that reaches a recall@10 of 0.95 on the
	// datasets the index was measured on. A search runs with the larger of ef
	// and its limit.
	DefaultEF = 20
	// DefaultEFConstruction is the ef of the search an insert finds its edges
	// with. MinimumEFConstruction is the number of edges a vector gets.
	DefaultEFConstruction = 160
	MinimumEFConstruction = 16
	// RescoreLimitAuto asks the index for the rescore limit that fits the
	// width of its codes. It is the default.
	RescoreLimitAuto    = -1
	DefaultRescoreLimit = RescoreLimitAuto
	// MaxPostingSizeAuto asks the index for the posting size it was measured
	// with, whatever the dimensions: a posting splits past 360 vectors, which
	// leaves a mean of 236. An explicit value is a size in KiB, converted to
	// vectors as HFresh converts its own maxPostingSizeKB.
	MaxPostingSizeAuto      = 0
	DefaultMaxPostingSizeKB = MaxPostingSizeAuto
	MinimumMaxPostingSizeKB = 8
	MaximumMaxPostingSizeKB = 1024
	// CacheModeRouting keeps in memory the code the postings are ranked by,
	// and reads the codes a posting is scanned by from disk. CacheModeNone
	// keeps no code in memory. It is a value of the parameter, and the index
	// does not have it yet.
	CacheModeRouting = "routing"
	CacheModeNone    = "none"
	DefaultCacheMode = CacheModeRouting
)

// RQConfig is the part of the configuration about the codes of the index,
// which are always centered 1-bit RQ: what is left to choose is how many
// candidates a search rescores and which codes stay in memory.
type RQConfig struct {
	RescoreLimit int    `json:"rescoreLimit"`
	CacheMode    string `json:"cacheMode"`
}

// UserConfig is the configuration of a posting+edges index in a class.
type UserConfig struct {
	Distance         string   `json:"distance"`
	EF               int      `json:"ef"`
	EFConstruction   int      `json:"efConstruction"`
	MaxPostingSizeKB int      `json:"maxPostingSizeKB"`
	RQ               RQConfig `json:"rq"`
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return IndexType
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

func (u UserConfig) IsMultiVector() bool {
	return false
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.Distance = vectorIndexCommon.DefaultDistanceMetric
	u.EF = DefaultEF
	u.EFConstruction = DefaultEFConstruction
	u.MaxPostingSizeKB = DefaultMaxPostingSizeKB
	u.RQ.RescoreLimit = DefaultRescoreLimit
	u.RQ.CacheMode = DefaultCacheMode
}

func NewDefaultUserConfig() UserConfig {
	var uc UserConfig
	uc.SetDefaults()
	return uc
}

// Validate tells whether the index can be created with the config. It is what
// the parsing of a class runs, so that a class the index cannot serve is never
// created.
func (u UserConfig) Validate() error {
	var errs []error

	if u.Distance != vectorIndexCommon.DistanceCosine {
		errs = append(errs, fmt.Errorf(
			"unsupported distance %q: the posting-edge index only supports %q in this version",
			u.Distance, vectorIndexCommon.DistanceCosine))
	}
	if u.EF < 1 {
		errs = append(errs, fmt.Errorf("ef is %d but must be at least 1", u.EF))
	}
	if u.EFConstruction < MinimumEFConstruction {
		errs = append(errs, fmt.Errorf("efConstruction is %d but must be at least %d",
			u.EFConstruction, MinimumEFConstruction))
	}
	if u.MaxPostingSizeKB != MaxPostingSizeAuto &&
		(u.MaxPostingSizeKB < MinimumMaxPostingSizeKB || u.MaxPostingSizeKB > MaximumMaxPostingSizeKB) {
		errs = append(errs, fmt.Errorf("maxPostingSizeKB is %d but must be %d (automatic) or between %d and %d",
			u.MaxPostingSizeKB, MaxPostingSizeAuto, MinimumMaxPostingSizeKB, MaximumMaxPostingSizeKB))
	}
	if u.RQ.RescoreLimit < RescoreLimitAuto {
		errs = append(errs, fmt.Errorf("rq.rescoreLimit is %d but must be %d (automatic) or at least 0",
			u.RQ.RescoreLimit, RescoreLimitAuto))
	}
	switch u.RQ.CacheMode {
	case CacheModeRouting:
	case CacheModeNone:
		errs = append(errs, fmt.Errorf("rq.cacheMode %q is not available yet: use %q",
			CacheModeNone, CacheModeRouting))
	default:
		errs = append(errs, unknownCacheMode(u.RQ.CacheMode))
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid posting-edge config: %w", errors.Join(errs...))
	}
	return nil
}

func unknownCacheMode(value interface{}) error {
	return fmt.Errorf("unknown rq.cacheMode %#v: the values are %q and %q", value, CacheModeRouting, CacheModeNone)
}

// parseRQ reads the rq block. The codes of the index are fixed, so anything
// that asks for other codes is an error, where the shared parsers would drop
// it without a word.
func parseRQ(ucMap map[string]interface{}, uc *UserConfig) error {
	rqValue, ok := ucMap["rq"]
	if !ok {
		return nil
	}
	rqMap, ok := rqValue.(map[string]interface{})
	if !ok {
		return fmt.Errorf("rq must be an object, got %T", rqValue)
	}

	enabled := true
	if err := vectorIndexCommon.OptionalBoolFromMap(rqMap, "enabled", func(v bool) {
		enabled = v
	}); err != nil {
		return err
	}
	if !enabled {
		return errors.New("the posting-edge index only works with rq: rq.enabled cannot be false")
	}

	bits := 1
	if err := vectorIndexCommon.OptionalIntFromMap(rqMap, "bits", func(v int) {
		bits = v
	}); err != nil {
		return err
	}
	if bits != 1 {
		return fmt.Errorf("the posting-edge index only supports rq with 1 bit, got %d", bits)
	}

	centering := true
	if err := vectorIndexCommon.OptionalBoolFromMap(rqMap, "centering", func(v bool) {
		centering = v
	}); err != nil {
		return err
	}
	if !centering {
		return errors.New("the codes of the posting-edge index are always centered: rq.centering cannot be false")
	}

	if err := vectorIndexCommon.OptionalIntFromMap(rqMap, "rescoreLimit", func(v int) {
		uc.RQ.RescoreLimit = v
	}); err != nil {
		return err
	}

	// A value of another type is not left as the default: flat has a boolean
	// under a similar name, and a boolean here is somebody expecting it.
	if mode, ok := rqMap["cacheMode"]; ok {
		asString, isString := mode.(string)
		if !isString {
			return unknownCacheMode(mode)
		}
		uc.RQ.CacheMode = asString
	}
	return nil
}

func parseQuantization(ucMap map[string]interface{}, uc *UserConfig) error {
	for _, other := range []string{
		vectorIndexCommon.CompressionPQ, vectorIndexCommon.CompressionSQ, vectorIndexCommon.CompressionBQ,
	} {
		if _, ok := ucMap[other]; ok {
			return fmt.Errorf("%s is not supported for the posting-edge index (only rq with 1 bit is)", other)
		}
	}
	return parseRQ(ucMap, uc)
}

// rejectMultivector refuses a class that asks the index for multi-vectors,
// through the vectorizer or through the config.
func rejectMultivector(ucMap map[string]interface{}, isMultiVector bool) error {
	enabled := isMultiVector
	if value, ok := ucMap["multivector"]; ok {
		if asMap, ok := value.(map[string]interface{}); ok {
			if err := vectorIndexCommon.OptionalBoolFromMap(asMap, "enabled", func(v bool) {
				enabled = enabled || v
			}); err != nil {
				return err
			}
		}
	}
	if enabled {
		return errors.New("multi-vectors are not supported by the posting-edge index")
	}
	return nil
}

// ParseAndValidateConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseAndValidateConfig(input interface{}, isMultiVector bool) (schemaConfig.VectorIndexConfig, error) {
	uc := UserConfig{}
	uc.SetDefaults()

	if input == nil {
		if isMultiVector {
			return uc, rejectMultivector(nil, isMultiVector)
		}
		return uc, nil
	}

	asMap, ok := input.(map[string]interface{})
	if !ok || asMap == nil {
		return uc, fmt.Errorf("input must be a non-nil map")
	}

	if err := rejectMultivector(asMap, isMultiVector); err != nil {
		return uc, err
	}
	if err := parseQuantization(asMap, &uc); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return uc, err
	}
	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "ef", func(v int) {
		uc.EF = v
	}); err != nil {
		return uc, err
	}
	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "efConstruction", func(v int) {
		uc.EFConstruction = v
	}); err != nil {
		return uc, err
	}
	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "maxPostingSizeKB", func(v int) {
		uc.MaxPostingSizeKB = v
	}); err != nil {
		return uc, err
	}

	return uc, uc.Validate()
}

// ValidateUserConfigUpdate tells whether a class can go from one config to
// another. What a search reads on every query can change at any time: ef and
// rq.rescoreLimit. Everything else shaped what is on disk.
func ValidateUserConfigUpdate(initial, updated schemaConfig.VectorIndexConfig) error {
	before, ok := initial.(UserConfig)
	if !ok {
		return fmt.Errorf("initial is not a posting-edge UserConfig, but %T", initial)
	}
	after, ok := updated.(UserConfig)
	if !ok {
		return fmt.Errorf("updated is not a posting-edge UserConfig, but %T", updated)
	}
	if err := after.Validate(); err != nil {
		return err
	}

	immutable := []struct {
		name          string
		before, after interface{}
	}{
		{"distance", before.Distance, after.Distance},
		{"efConstruction", before.EFConstruction, after.EFConstruction},
		{"maxPostingSizeKB", before.MaxPostingSizeKB, after.MaxPostingSizeKB},
		{"rq.cacheMode", before.RQ.CacheMode, after.RQ.CacheMode},
	}
	for _, field := range immutable {
		if field.before != field.after {
			return fmt.Errorf("%s is immutable: attempted change from \"%v\" to \"%v\"",
				field.name, field.before, field.after)
		}
	}
	return nil
}
