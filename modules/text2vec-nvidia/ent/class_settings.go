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

package ent

import (
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultBaseURL               = "https://integrate.api.nvidia.com"
	DefaultNvidiaModel           = "nvidia/nv-embed-v1"
	DefaultTruncate              = "NONE"
	DefaultVectorizeClassName    = false
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	LowerCaseInput               = false
)

var availableTruncates = []string{"NONE", "START", "END"}

type classSettings struct {
	basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: *basesettings.NewBaseClassSettings(cfg, LowerCaseInput)}
}

func (cs *classSettings) BaseURL() string {
	return cs.BaseClassSettings.GetPropertyAsString("baseURL", DefaultBaseURL)
}

func (cs *classSettings) Model() string {
	return cs.BaseClassSettings.GetPropertyAsString("model", DefaultNvidiaModel)
}

func (cs *classSettings) Truncate() *string {
	truncate := cs.BaseClassSettings.GetPropertyAsString("truncate", DefaultTruncate)
	return &truncate
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.BaseClassSettings.Validate(class); err != nil {
		return err
	}

	truncate := cs.Truncate()
	if truncate != nil && !basesettings.ValidateSetting[string](*truncate, availableTruncates) {
		return errors.Errorf("wrong truncate type, available types are: %v", availableTruncates)
	}

	return nil
}
