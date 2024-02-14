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
)

type indexChecker struct {
	*objectsvectorizer.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewIndexChecker(cfg moduletools.ClassConfig) *indexChecker {
	return &indexChecker{
		BaseClassSettings: objectsvectorizer.NewBaseClassSettings(cfg, &objectsvectorizer.ClassSettingDefaults{
			DefaultVectorizeClassName:     DefaultVectorizeClassName,
			DefaultPropertyIndexed:        DefaultPropertyIndexed,
			DefaultVectorizePropertyName:  DefaultVectorizePropertyName,
			DefaultLowerCasePropertyValue: true,
		}),
		cfg: cfg,
	}
}
