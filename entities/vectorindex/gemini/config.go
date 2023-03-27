
package gemini

import (

    "github.com/weaviate/weaviate/entities/schema"
)


// The Gemini Plugin currently takes most configurable settings through 
// env vars ( see the plugin code at gsi/weaviate_gemini_plugin/ ).
// This class is a miminal stub required by the Weavaite code-base.
type UserConfig struct {
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
    return "gemini"
}

    
// The Gemini Plugin currently takes most configurable settings through 
// env vars ( see the plugin code at gsi/weaviate_gemini_plugin/ ).
// This class is a miminal stub required by the Weavaite code-base.
func ParseUserConfig(input interface{}) (schema.VectorIndexConfig, error) {

    uc := UserConfig{}
    return uc, nil
}
 
