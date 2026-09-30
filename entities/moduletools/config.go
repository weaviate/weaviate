//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package moduletools

import (
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// ClassConfig is a helper type which is passed to the module to read it's
// per-class config. This is - among other places - used when vectorizing and
// when validation schema config
type ClassConfig interface {
	TargetVector() string
	Tenant() string
	Class() map[string]any
	ClassByModuleName(moduleName string) map[string]any
	Property(propName string) map[string]any
	PropertiesDataTypes() map[string]schema.DataType
	Config() *config.Config
}
