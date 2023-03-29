
package gemini

import (

    "github.com/weaviate/weaviate/entities/schema"
)

// A minimal number of config parameters arre available here in UserConfig.
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

// SetDefaults in the user-specifyable part of the config
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
 
