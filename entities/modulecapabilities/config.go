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

package modulecapabilities

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

// ClassConfigurator is an optional capability interface which a module MAY
// implement. If it is implemented, all methods will be called when the user
// adds or updates a class which has the module set as the vectorizer
type ClassConfigurator interface {
	// ClassDefaults provides the defaults for a per-class module config. The
	// module provider will merge the props into the user-specified config with
	// the user-provided values taking precedence
	ClassConfigDefaults() map[string]any

	// PropertyConfigDefaults provides the defaults for a per-property module
	// config. The module provider will merge the props into the user-specified
	// config with the user-provided values taking precedence. The property's
	// dataType MAY be taken into consideration when deciding defaults.
	// dataType is not guaranteed to be non-nil, it might be nil in the case a
	// user specified an invalid dataType, as some validation only occurs after
	// defaults are set.
	PropertyConfigDefaults(dataType *schema.DataType) map[string]any

	// ValidateClass MAY validate anything about the class, except the config of
	// another module. The specified ClassConfig can be used to easily retrieve
	// the config specific for the module. For example, a module could iterate
	// over class.Properties and call classConfig.Property(prop.Name) to validate
	// the per-property config. A module MUST NOT extract another module's config
	// from class.ModuleConfig["other-modules-name"].
	ValidateClass(ctx context.Context, class *models.Class,
		classConfig moduletools.ClassConfig) error
}

// MigrateProperty defines module settings property name migration.
// Example #1:
// { Name: "baseUrl", NewName: "baseURL" }
// This definition means that if a class config contains a property
// with an old name, then it's value will be assigned to new name
// and the old name will be removed for class's config.
// Example #2:
// { Name: "baseURL" }
// This definition means that a new property with name baseURL
// was added to configuration and we need to add it to config
// Example #3:
// { Name: "baseURL", IsDeleted: true }
// This definition means that a property with name baseURL
// was deleted from default configuration and we need to remove it
type MigrateProperty struct {
	Name, NewName string
	IsDeleted     bool
}

// MigrateProperties interface enables module settings property names to be migrated
// from old names to new names.
type MigrateProperties interface {
	MigrateProperties() []MigrateProperty
}
