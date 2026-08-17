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

// reportUnmatchedKeyDocColumns says which configured key/doc columns the schema
// has nothing to build. A name that matches nothing costs nothing and does
// nothing — the node starts, the property serves ContainsAny from the standard
// fold, and the only sign an operator asked for anything else is a debug line
// per bucket that was never written.
//
// Once, against the schema, rather than per shard: the shard has the same
// information, but a collection with ten thousand tenants would say the same
// thing ten thousand times. So this is bounded by what was configured, and a
// collection created after startup is not revisited — it answers "is this
// configuration about anything", not "is it still".
func reportUnmatchedKeyDocColumns(logger logrus.FieldLogger,
	configured map[string][]string, classes []*models.Class,
) {
	if len(configured) == 0 {
		return
	}

	byName := make(map[string]*models.Class, len(classes))
	for _, class := range classes {
		byName[class.Class] = class
	}

	for className, props := range configured {
		log := logger.WithFields(logrus.Fields{
			"action":     "keydoccolumn_config",
			"collection": className,
		})

		class, ok := byName[className]
		if !ok {
			log.Warnf("%s names a collection this node's schema does not have; "+
				"nothing is built for it", config.KeyDocColumnEnv)
			continue
		}

		built, reported := 0, 0
		for _, propName := range props {
			if propName == config.AllProperties {
				for _, prop := range class.Properties {
					if whyNoKeyDocColumn(class, prop.Name) == "" {
						built++
					}
				}
				continue
			}
			if reason := whyNoKeyDocColumn(class, propName); reason != "" {
				log.WithField("property", propName).Warnf(
					"%s names a property that gets no key/doc column: %s. "+
						"ContainsAny on it uses the standard fold",
					config.KeyDocColumnEnv, reason)
				reported++
				continue
			}
			built++
		}

		// Only where nothing above spoke: a named property that gets nothing has
		// already said so, with a reason. What is left is the wildcard, which
		// names no property to report on and is the shorter thing to type — so
		// without this, a collection whose properties are all ineligible is the
		// one case the report is silent about.
		if built == 0 && reported == 0 {
			log.Warnf("%s names %q, which has no property that gets a key/doc "+
				"column; nothing is built for it", config.KeyDocColumnEnv, className)
		}
	}
}

// whyNoKeyDocColumn reports why propName gets no column, or "" if it does.
//
// The reasons come from [inverted.WhyNoKeyDocColumn], which the query path
// routes on, so this cannot promise a column the query side would refuse to
// read. Phrased for an operator here, since what the query path carries is a
// slug for a log field.
//
// Only what the schema decides: a build that reads the property's values and
// declines them says so itself, from the bucket it declined.
func whyNoKeyDocColumn(class *models.Class, propName string) string {
	// against the name the schema stores: a property written "SKU" is held as
	// "sKU", and the query path normalises the same way
	wanted := schema.LowercaseFirstLetter(propName)
	for _, prop := range class.Properties {
		if prop.Name != wanted {
			continue
		}
		// The schema's own tokenization, not a resolved one: a retokenization in
		// flight is a shard's business, and this runs once for the node.
		return keyDocColumnReasonText(inverted.WhyNoKeyDocColumn(prop, prop.Tokenization))
	}
	return "the collection has no such property"
}

// keyDocColumnReasonText says, in an operator's terms, what one of
// [inverted.WhyNoKeyDocColumn]'s reasons means. An unknown one is passed
// through rather than swallowed: a reason added on the query side should show
// up here as itself, not as silence.
func keyDocColumnReasonText(reason string) string {
	switch reason {
	case "":
		return ""
	case "no-filterable-index":
		return "the property has no filterable index"
	case "geo-property":
		return "geo properties are served by their own index"
	case "reference-property":
		return "reference properties are not filtered this way"
	case "nested-object-property":
		return "nested-object properties are not filtered this way"
	case "tokenization-not-field":
		return `only text properties with tokenization "field" resolve one value to one key`
	default:
		return reason
	}
}
