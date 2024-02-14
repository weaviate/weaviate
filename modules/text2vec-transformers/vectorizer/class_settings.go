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

package vectorizer

import (
	"github.com/weaviate/weaviate/entities/moduletools"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
)

const (
	DefaultPropertyIndexed       = true
	DefaultVectorizeClassName    = true
	DefaultVectorizePropertyName = false
	DefaultPoolingStrategy       = "masked_mean"
)

type classSettings struct {
	*objectsvectorizer.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{
		BaseClassSettings: objectsvectorizer.NewBaseClassSettings(cfg, &objectsvectorizer.ClassSettingDefaults{
			DefaultVectorizeClassName:     DefaultVectorizeClassName,
			DefaultPropertyIndexed:        DefaultPropertyIndexed,
			DefaultVectorizePropertyName:  DefaultVectorizePropertyName,
			DefaultLowerCasePropertyValue: true,
		}),
		cfg: cfg}
}

func (ic *classSettings) PoolingStrategy() string {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return DefaultPoolingStrategy
	}

	vcn, ok := ic.cfg.Class()["poolingStrategy"]
	if !ok {
		return DefaultPoolingStrategy
	}

	asString, ok := vcn.(string)
	if !ok {
		return DefaultPoolingStrategy
	}

	return asString
}
