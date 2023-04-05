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

    "github.com/weaviate/weaviate/entities/schema"
)

// A minimal number of gemini index config parameters are available here in UserConfig.
const (
    DefaultSkip                 = false
    DefaultSearchType           = "flat"
    DefaultCentroidsHammingK    = 5000
    DefaultCentroidsRerank      = 4000
    DefaultHammingK             = 3200
    DefaultNBits                = 768
)

type UserConfig struct {
    Skip                    bool   `json:"skip"`
    SearchType              string `json:"distance"`
    CentroidsHammingK       int    `json:"centroidsHammingK"`
    CentroidsRerank         int    `json:"centroidsRerank"`
    HammingK                int    `json:"hammingK"`
    NBits                   int    `json:"nBits"`
}

func (u UserConfig) IndexType() string {
    return "gemini"
}

func (c *UserConfig) SetDefaults() {
    c.Skip              = DefaultSkip
    c.SearchType        = DefaultSearchType
    c.CentroidsHammingK = DefaultCentroidsHammingK
    c.CentroidsRerank   = DefaultCentroidsRerank
    c.HammingK          = DefaultHammingK
    c.NBits             = DefaultNBits
}

    
func ParseUserConfig(input interface{}) (schema.VectorIndexConfig, error) {
    uc := UserConfig{}
    uc.SetDefaults()
    return uc, nil
}
 
