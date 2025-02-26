//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build cuvs

package cuvs

import (
	"fmt"
	"strings"

	vectorIndexCommon "github.com/weaviate/weaviate/entities/vectorindex/common"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

type UserConfig struct {
	GraphDegree             int    `json:"graphDegree"`
	IntermediateGraphDegree int    `json:"intermediateGraphDegree"`
	BuildAlgo               string `json:"buildAlgo"`
	SearchAlgo              string `json:"searchAlgo"`
	ItopKSize               int    `json:"itopKSize"`
	SearchWidth             int    `json:"searchWidth"`
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "cuvs"
}

func (u UserConfig) DistanceName() string {
	return "l2-squared"
}

func (u UserConfig) IsMultiVector() bool {
	return false
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.GraphDegree = 32
	u.IntermediateGraphDegree = 32
	u.BuildAlgo = "nn_descent"
	u.SearchAlgo = "multi_cta"
	u.ItopKSize = 256
	u.SearchWidth = 1
}

// ParseAndValidateConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseAndValidateConfig(input interface{}) (schemaConfig.VectorIndexConfig, error) {
	uc := UserConfig{}
	uc.SetDefaults()

	if input == nil {
		return uc, nil
	}

	asMap, ok := input.(map[string]interface{})
	if !ok || asMap == nil {
		return uc, fmt.Errorf("input must be a non-nil map")
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "graphDegree", func(v int) {
		uc.GraphDegree = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "intermediateGraphDegree", func(v int) {
		uc.IntermediateGraphDegree = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalStringFromMap(asMap, "buildAlgo", func(v string) {
		uc.BuildAlgo = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalStringFromMap(asMap, "searchAlgo", func(v string) {
		uc.SearchAlgo = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "itopKSize", func(v int) {
		uc.ItopKSize = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "searchWidth", func(v int) {
		uc.SearchWidth = v
	}); err != nil {
		return uc, err
	}

	return uc, uc.validate()
}

func (u *UserConfig) validate() error {
	var errMsgs []string
	if u.BuildAlgo != "nn_descent" && u.BuildAlgo != "ivf_pq" && u.BuildAlgo != "auto_select" {
		errMsgs = append(errMsgs, fmt.Sprintf(
			"buildAlgo must be one of 'nn_descent', 'ivf_pq' or 'auto_select', but %s was given",
			u.BuildAlgo,
		))
	}

	if u.SearchAlgo != "multi_cta" && u.SearchAlgo != "single_cta" {
		errMsgs = append(errMsgs, fmt.Sprintf(
			"searchAlgo must be one of 'multi_cta' or 'single_cta', but %s was given",
			u.SearchAlgo,
		))
	}

	if len(errMsgs) > 0 {
		return fmt.Errorf("invalid hnsw config: %s",
			strings.Join(errMsgs, ", "))
	}

	return nil
}

func NewDefaultUserConfig() UserConfig {
	uc := UserConfig{}
	uc.SetDefaults()
	return uc
}
