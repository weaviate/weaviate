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

package hfresh

import (
	"errors"
	"fmt"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	vectorIndexCommon "github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	DefaultMuveraKSim         = hnsw.DefaultMultivectorKSim
	DefaultMuveraDProjections = hnsw.DefaultMultivectorDProjections
	DefaultMuveraRepetitions  = hnsw.DefaultMultivectorRepetitions
)

const (
	DefaultMaxPostingSizeKB     = 48
	MaxPostingSizeKBFloor       = 8
	DefaultReplicas             = 4
	DefaultSearchProbe          = 64
	DefaultHFreshRescoreLimit   = 350
	MaximumAllowedReplicas      = 10
	MaximumAllowedPostingSizeKB = 1024
)

// UserConfig defines the configuration options for the HFresh index.
// Will be populated once we decide what should be exposed.
type UserConfig struct {
	MaxPostingSizeKB uint32                 `json:"maxPostingSizeKB"`
	Replicas         uint32                 `json:"replicas"`
	SearchProbe      uint32                 `json:"searchProbe"`
	Distance         string                 `json:"distance"`
	RQ               hnsw.RQConfig          `json:"rq"`
	Multivector      hnsw.MultivectorConfig `json:"multivector"`
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "hfresh"
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

func (u UserConfig) IsMultiVector() bool {
	return u.Multivector.MuveraConfig.Enabled
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.MaxPostingSizeKB = DefaultMaxPostingSizeKB
	u.Replicas = DefaultReplicas
	u.SearchProbe = DefaultSearchProbe
	u.Distance = vectorIndexCommon.DefaultDistanceMetric
	u.RQ.Enabled = true
	u.RQ.Bits = 1
	u.RQ.RescoreLimit = DefaultHFreshRescoreLimit
	u.Multivector.Enabled = false
	u.Multivector.MuveraConfig.Enabled = false
	u.Multivector.MuveraConfig.KSim = DefaultMuveraKSim
	u.Multivector.MuveraConfig.DProjections = DefaultMuveraDProjections
	u.Multivector.MuveraConfig.Repetitions = DefaultMuveraRepetitions
}

func NewDefaultUserConfig() UserConfig {
	var uc UserConfig
	uc.SetDefaults()
	return uc
}

func (u *UserConfig) validate() error {
	var errs []error

	if u.Distance != vectorIndexCommon.DistanceCosine && u.Distance != vectorIndexCommon.DistanceL2Squared {
		errs = append(errs, fmt.Errorf(
			"unsupported distance type '%s', HFresh only supports 'cosine' or 'l2-squared' for the distance metric",
			u.Distance,
		))
	}

	if u.MaxPostingSizeKB < MaxPostingSizeKBFloor {
		errs = append(errs, fmt.Errorf(
			"maxPostingSizeKB is '%d' but must be at least %d",
			u.MaxPostingSizeKB,
			MaxPostingSizeKBFloor,
		))
	}

	if u.Replicas > MaximumAllowedReplicas {
		errs = append(errs, fmt.Errorf(
			"replicas is '%d' but must be less than %d",
			u.Replicas,
			MaximumAllowedReplicas,
		))
	}

	if u.MaxPostingSizeKB > MaximumAllowedPostingSizeKB {
		errs = append(errs, fmt.Errorf(
			"maxPostingSizeKB is '%d' but must be less than %d",
			u.MaxPostingSizeKB,
			MaximumAllowedPostingSizeKB,
		))
	}

	if u.Multivector.MuveraConfig.Enabled {
		if u.Multivector.MuveraConfig.KSim <= 0 {
			errs = append(errs, fmt.Errorf("muvera ksim must be greater than 0"))
		}
		if u.Multivector.MuveraConfig.DProjections <= 0 {
			errs = append(errs, fmt.Errorf("muvera dprojections must be greater than 0"))
		}
		if u.Multivector.MuveraConfig.Repetitions <= 0 {
			errs = append(errs, fmt.Errorf("muvera repetitions must be greater than 0"))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid hfresh config: %w", errors.Join(errs...))
	}

	return nil
}

func parseAndValidateRQ(ucMap map[string]interface{}, uc *UserConfig) error {
	rqConfigValue, ok := ucMap["rq"]
	if !ok {
		return nil
	}

	rqConfigMap, ok := rqConfigValue.(map[string]interface{})
	if !ok {
		return nil
	}

	enabled := true
	if err := vectorIndexCommon.OptionalBoolFromMap(rqConfigMap, "enabled", func(v bool) {
		enabled = v
	}); err != nil {
		return err
	}
	if !enabled {
		return fmt.Errorf("hfresh only supports rq")
	}

	var bits int
	if err := vectorIndexCommon.OptionalIntFromMap(rqConfigMap, "bits", func(v int) {
		bits = v
	}); err != nil {
		return err
	}
	if bits > 1 {
		return fmt.Errorf("rq only supports 1 bit, got %d", bits)
	}

	if err := vectorIndexCommon.OptionalIntFromMap(rqConfigMap, "rescoreLimit", func(v int) {
		if v >= 0 {
			uc.RQ.RescoreLimit = v
		}
	}); err != nil {
		return err
	}

	return nil
}

func parseAndValidateQuantization(ucMap map[string]interface{}, uc *UserConfig) error {
	if _, ok := ucMap["pq"]; ok {
		return fmt.Errorf("pq is not supported for hfresh index (only rq-1 is supported)")
	}
	if _, ok := ucMap["sq"]; ok {
		return fmt.Errorf("sq is not supported for hfresh index (only rq-1 is supported)")
	}
	if _, ok := ucMap["bq"]; ok {
		return fmt.Errorf("bq is not supported for hfresh index (only rq-1 is supported)")
	}
	return parseAndValidateRQ(ucMap, uc)
}

// ParseAndValidateConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
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

	if err := parseAndValidateQuantization(asMap, &uc); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "maxPostingSizeKB", func(v int) {
		uc.MaxPostingSizeKB = uint32(v)
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "replicas", func(v int) {
		uc.Replicas = uint32(v)
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "searchProbe", func(v int) {
		uc.SearchProbe = uint32(v)
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return uc, err
	}

	if err := parseMultivectorConfig(asMap, &uc.Multivector, isMultiVector); err != nil {
		return uc, err
	}

	return uc, uc.validate()
}

func parseMultivectorConfig(in map[string]interface{}, multivector *hnsw.MultivectorConfig, isMultiVector bool) error {
	multivectorValue, ok := in["multivector"]
	if !ok {
		return nil
	}

	multivectorMap, ok := multivectorValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := vectorIndexCommon.OptionalBoolFromMap(multivectorMap, "enabled", func(v bool) {
		if isMultiVector {
			multivector.Enabled = true
		} else {
			multivector.Enabled = v
		}
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

	if err := vectorIndexCommon.OptionalBoolFromMap(muveraMap, "enabled", func(v bool) {
		multivector.MuveraConfig.Enabled = v
		if v {
			multivector.Enabled = true
		}
	}); err != nil {
		return err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(muveraMap, "ksim", func(v int) {
		multivector.MuveraConfig.KSim = v
	}); err != nil {
		return err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(muveraMap, "dprojections", func(v int) {
		multivector.MuveraConfig.DProjections = v
	}); err != nil {
		return err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(muveraMap, "repetitions", func(v int) {
		multivector.MuveraConfig.Repetitions = v
	}); err != nil {
		return err
	}

	return nil
}
