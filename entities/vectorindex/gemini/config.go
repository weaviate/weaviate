
package gemini

import (

    "github.com/weaviate/weaviate/entities/schema"
)

// The minimal number of parameters available in UserConfig.
// See the gemini plugin module for the full suite of defaults
// that we make make available at a later time.
const (
    DefaultSkip                 = false
    DefaultCentroidsHammingK    = 5000
    DefaultCentroidsRerank      = 4000
    DefaultHammingK             = 3200
    DefaultNBits                = 768
)

type UserConfig struct {
    Skip                    bool   `json:"skip"`
    CentroidsHammingK       int    `json:"centroidsHammingK"`
    CentroidsRerank         int    `json:"centroidsRerank"`
    HammingK                int    `json:"hammingK"`
    NBits                   int    `json:"nBits"`
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
    return "gemini"
}

// SetDefaults in the user-specifyable part of the config
func (c *UserConfig) SetDefaults() {
    c.Skip              = DefaultSkip
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
 
