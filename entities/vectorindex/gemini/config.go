//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package gemini

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

// A minimal number of gemini index config parameters are available here in UserConfig.
const (
	DefaultSkip              = false
	DefaultSearchType        = "flat"
	DefaultCentroidsHammingK = 5000
	DefaultCentroidsRerank   = 4000
	DefaultHammingK          = 3200
	DefaultNBits             = 768
)

const (
	GeminiSearchTypeFlat     = "flat"
	GeminiSearchTypeClusters = "clusters"
)

type UserConfig struct {
	Skip              bool   `json:"skip"`
	SearchType        string `json:"searchType"`
	CentroidsHammingK int    `json:"centroidsHammingK"`
	CentroidsRerank   int    `json:"centroidsRerank"`
	HammingK          int    `json:"hammingK"`
	NBits             int    `json:"nBits"`
}

func (u UserConfig) IndexType() string {
	return "gemini"
}

func (c *UserConfig) SetDefaults() {
	c.Skip = DefaultSkip
	c.SearchType = DefaultSearchType
	c.CentroidsHammingK = DefaultCentroidsHammingK
	c.CentroidsRerank = DefaultCentroidsRerank
	c.HammingK = DefaultHammingK
	c.NBits = DefaultNBits
}

func ParseUserConfig(input interface{}) (schema.VectorIndexConfig, error) {
	uc := UserConfig{}
	uc.SetDefaults()

	// TODO: Currently we are only allow the setting of nbits and searchtype

	dct := input.(map[string]interface{})
	// configure nBits
	dval := dct["nBits"]
	val, err := dval.(json.Number).Int64()
	if err != nil {
		return nil, errors.Wrapf(err, "Could not parse user config 'nBits'.")
	}
	uc.NBits = int(val)

	// configure searchType
	sdval := dct["searchType"]
	stval, sterr := sdval.(string)
	if !sterr {
		return nil, errors.Wrapf(err, "Could not parse user config 'searchType'.")
	}
	uc.SearchType = stval

	return uc, nil
}
