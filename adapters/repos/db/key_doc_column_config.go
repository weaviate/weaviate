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

package db

import (
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// reportKeyDocColumnConfig warns about each key/doc column names configures on
// class that builds nothing. [Migrator.AddClass] calls it once per index, so a
// collection of many tenants warns once.
func reportKeyDocColumnConfig(logger logrus.FieldLogger, names []string, class *models.Class) {
	if len(names) == 0 {
		return
	}
	log := keyDocColumnConfigLogger(logger, class.Class)

	built, reported := 0, 0
	for _, name := range names {
		if name == config.AllProperties {
			for _, prop := range class.Properties {
				if keyDocColumnRefusalText(prop) == "" {
					built++
				}
			}
			continue
		}
		prop := findKeyDocColumnProp(class, name)
		if prop == nil {
			log.WithField("property", name).Warnf("%s names a property this collection "+
				"does not have; a column is built if one is added", config.KeyDocColumnEnv)
			reported++
			continue
		}
		if reason := keyDocColumnRefusalText(prop); reason != "" {
			warnNoKeyDocColumn(log, name, reason)
			reported++
			continue
		}
		built++
	}

	// The wildcard names no property, so a collection whose properties are all
	// ineligible would otherwise go unreported.
	if built == 0 && reported == 0 {
		log.Warnf("%s names %q, which has no property that gets a key/doc column yet",
			config.KeyDocColumnEnv, class.Class)
	}
}

// reportAddedKeyDocColumnProps warns about each of props, just added to className,
// that names lists explicitly and that gets no key/doc column.
func reportAddedKeyDocColumnProps(logger logrus.FieldLogger, names []string,
	className string, props []*models.Property,
) {
	for _, prop := range props {
		if !namesKeyDocColumnProp(names, prop.Name) {
			continue
		}
		if reason := keyDocColumnRefusalText(prop); reason != "" {
			warnNoKeyDocColumn(keyDocColumnConfigLogger(logger, className), prop.Name, reason)
		}
	}
}

func keyDocColumnConfigLogger(logger logrus.FieldLogger, className string) logrus.FieldLogger {
	return logger.WithFields(logrus.Fields{
		"action":     "keydoccolumn_config",
		"collection": className,
	})
}

func warnNoKeyDocColumn(log logrus.FieldLogger, propName, reason string) {
	log.WithField("property", propName).Warnf(
		"%s names a property that gets no key/doc column: %s. "+
			"ContainsAny on it uses the standard fold",
		config.KeyDocColumnEnv, reason)
}

// namesKeyDocColumnProp reports whether names lists propName itself. It compares
// against the name the schema stores, since a property written "SKU" is held as
// "sKU" and the query path normalises the same way.
func namesKeyDocColumnProp(names []string, propName string) bool {
	for _, name := range names {
		if schema.LowercaseFirstLetter(name) == propName {
			return true
		}
	}
	return false
}

func findKeyDocColumnProp(class *models.Class, name string) *models.Property {
	wanted := schema.LowercaseFirstLetter(name)
	for _, prop := range class.Properties {
		if prop.Name == wanted {
			return prop
		}
	}
	return nil
}

// keyDocColumnRefusalText says, for an operator, why the schema gives prop no
// column, or "" if it does. It defers to [inverted.WhyNotBatchable], which routes
// queries; a build that declines the values warns from its own bucket.
func keyDocColumnRefusalText(prop *models.Property) string {
	// This passes the schema's tokenization, since a retokenization in flight
	// is resolved per shard.
	return keyDocColumnReasonText(inverted.WhyNotBatchable(prop, prop.Tokenization))
}

// keyDocColumnReasonText phrases an [inverted.WhyNotBatchable] reason for an
// operator, passing an unknown one through unchanged.
func keyDocColumnReasonText(reason string) string {
	switch reason {
	case "":
		return ""
	case inverted.ContainsDeclineNoFilterableIndex:
		return "the property has no filterable index"
	case inverted.ContainsDeclineGeoProperty:
		return "geo properties are served by their own index"
	case inverted.ContainsDeclineReferenceProperty:
		return "reference properties are not filtered this way"
	case inverted.ContainsDeclineNestedObjectProperty:
		return "nested-object properties are not filtered this way"
	case inverted.ContainsDeclineTokenizationNotField:
		return `only text properties with tokenization "field" resolve one value to one key`
	default:
		return reason
	}
}
