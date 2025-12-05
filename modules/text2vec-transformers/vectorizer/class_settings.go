//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package vectorizer

import (
	"errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultPropertyIndexed       = true
	DefaultVectorizeClassName    = true
	DefaultVectorizePropertyName = false
	DefaultPoolingStrategy       = "masked_mean"

	// Parameter keys for accessing the Parameters map
	ParamPoolingStrategy     = "PoolingStrategy"
	ParamInferenceURL        = "InferenceURL"
	ParamPassageInferenceURL = "PassageInferenceURL"
	ParamQueryInferenceURL   = "QueryInferenceURL"
	ParamDimensions          = "Dimensions"
)

// Parameters defines all configuration parameters for text2vec-transformers
var Parameters = map[string]basesettings.ParameterDef{
	ParamPoolingStrategy: {
		JSONKey:      "poolingStrategy",
		DefaultValue: DefaultPoolingStrategy,
		Description:  "Pooling strategy for sentence embeddings",
		Required:     false,
	},
	ParamInferenceURL: {
		JSONKey:      "inferenceUrl",
		DefaultValue: "",
		Description:  "Inference API URL for the transformer model",
		Required:     false,
	},
	ParamPassageInferenceURL: {
		JSONKey:      "passageInferenceUrl",
		DefaultValue: "",
		Description:  "Inference API URL for passage encoding (used with queryInferenceUrl)",
		Required:     false,
	},
	ParamQueryInferenceURL: {
		JSONKey:      "queryInferenceUrl",
		DefaultValue: "",
		Description:  "Inference API URL for query encoding (used with passageInferenceUrl)",
		Required:     false,
	},
	ParamDimensions: {
		JSONKey:      "dimensions",
		DefaultValue: nil,
		Description:  "Number of dimensions for the embedding",
		Required:     false,
	},
}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg, false)}
}

func (ic *classSettings) PoolingStrategy() string {
	return ic.BaseClassSettings.GetPropertyAsString(Parameters[ParamPoolingStrategy].JSONKey, DefaultPoolingStrategy)
}

func (ic *classSettings) InferenceURL() string {
	return ic.BaseClassSettings.GetPropertyAsString(Parameters[ParamInferenceURL].JSONKey, "")
}

func (ic *classSettings) PassageInferenceURL() string {
	return ic.BaseClassSettings.GetPropertyAsString(Parameters[ParamPassageInferenceURL].JSONKey, "")
}

func (ic *classSettings) QueryInferenceURL() string {
	return ic.BaseClassSettings.GetPropertyAsString(Parameters[ParamQueryInferenceURL].JSONKey, "")
}

func (ic *classSettings) Dimensions() *int64 {
	return ic.BaseClassSettings.GetPropertyAsInt64(Parameters[ParamDimensions].JSONKey, nil)
}

func (ic *classSettings) Validate(class *models.Class) error {
	if err := ic.BaseClassSettings.Validate(class); err != nil {
		return err
	}
	if ic.InferenceURL() != "" && (ic.PassageInferenceURL() != "" || ic.QueryInferenceURL() != "") {
		return errors.New("either inferenceUrl or passageInferenceUrl together with queryInferenceUrl needs to be set, not both")
	}
	if ic.PassageInferenceURL() == "" && ic.QueryInferenceURL() != "" {
		return errors.New("queryInferenceUrl is set but passageInferenceUrl is empty, both needs to be set")
	}
	if ic.PassageInferenceURL() != "" && ic.QueryInferenceURL() == "" {
		return errors.New("passageInferenceUrl is set but queryInferenceUrl is empty, both needs to be set")
	}
	return nil
}
