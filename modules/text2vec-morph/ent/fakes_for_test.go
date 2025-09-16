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

package ent

import (
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

type fakeClassConfig struct {
	classConfig           map[string]any
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
}

func (f fakeClassConfig) Class() map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]any {
	if propName == f.skippedProperty {
		return map[string]any{
			"skip": true,
		}
	}
	if propName == f.excludedProperty {
		return map[string]any{
			"vectorizePropertyName": false,
		}
	}
	if f.vectorizePropertyName {
		return map[string]any{
			"vectorizePropertyName": true,
		}
	}
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func (f fakeClassConfig) Config() *config.Config {
	return nil
}
