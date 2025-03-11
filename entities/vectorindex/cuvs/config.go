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
	Distance                string `json:"distance"`
	GraphDegree             int    `json:"graphDegree"`
	IntermediateGraphDegree int    `json:"intermediateGraphDegree"`
	BuildAlgo               string `json:"buildAlgo"`
	SearchAlgo              string `json:"searchAlgo"`
	ItopKSize               int    `json:"itopKSize"`
	SearchWidth             int    `json:"searchWidth"`
	IndexLocation           string `json:"indexLocation"`
	FilterDeleteLimit       int    `json:"filterDeleteLimit"`
	ExtendLimit             int    `json:"extendLimit"`
	BatchEnabled            bool   `json:"batchEnabled"`
	BatchSize               int    `json:"batchSize"`
	BatchMaxWaitMs          int    `json:"batchMaxWaitMs"`
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "cuvs"
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

func (u UserConfig) IsMultiVector() bool {
	return false
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.Distance = "l2-squared"
	u.GraphDegree = 32
	u.IntermediateGraphDegree = 32
	u.BuildAlgo = "nn_descent"
	// why 20? https://weaviate-org.slack.com/archives/C05V3MGDGQY/p1722897390825229?thread_ts=1722894509.398509&cid=C05V3MGDGQY
	u.FilterDeleteLimit = 30
	u.ExtendLimit = 20
	// updateable
	u.SearchAlgo = "multi_cta"
	u.ItopKSize = 256
	u.SearchWidth = 1
	u.IndexLocation = "gpu"
	u.BatchEnabled = false
	u.BatchSize = 100
	u.BatchMaxWaitMs = 10
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

	if err := vectorIndexCommon.OptionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return uc, err
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
	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "filterDeleteLimit", func(v int) {
		uc.FilterDeleteLimit = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "extendLimit", func(v int) {
		uc.ExtendLimit = v
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

	if err := vectorIndexCommon.OptionalStringFromMap(asMap, "indexLocation", func(v string) {
		uc.IndexLocation = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalBoolFromMap(asMap, "batchEnabled", func(v bool) {
		uc.BatchEnabled = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "batchSize", func(v int) {
		uc.BatchSize = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "batchMaxWaitMs", func(v int) {
		uc.BatchMaxWaitMs = v
	}); err != nil {
		return uc, err
	}

	return uc, uc.validate()
}

func (u *UserConfig) validate() error {
	var errMsgs []string

	if u.Distance != "l2-squared" && u.Distance != "dot" {
		errMsgs = append(errMsgs, fmt.Sprintf(
			"distance must be one of 'l2-squared' or 'dot', but %s was given",
			u.Distance,
		))
	}

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

	if u.ExtendLimit < 0 || u.ExtendLimit > 100 {
		errMsgs = append(errMsgs, fmt.Sprintf(
			"extendLimit must be between 0 and 100, but %d was given",
			u.ExtendLimit,
		))
	}

	if u.FilterDeleteLimit < 0 || u.FilterDeleteLimit > 100 {
		errMsgs = append(errMsgs, fmt.Sprintf(
			"filterDeleteLimit must be between 0 and 100, but %d was given",
			u.FilterDeleteLimit,
		))
	}

	if u.IndexLocation != "gpu" && u.IndexLocation != "cpu" {
		errMsgs = append(errMsgs, fmt.Sprintf(
			"indexLocation must be one of 'gpu' or 'cpu', but %s was given",
			u.IndexLocation,
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
