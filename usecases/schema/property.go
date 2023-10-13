//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/schema"

	"github.com/weaviate/weaviate/entities/models"
)

// AddClassProperty to an existing Class
func (m *Handler) AddClassProperty(ctx context.Context, principal *models.Principal,
	class string, prop *models.Property,
) error {
	err := m.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	if prop.Name == "" {
		return fmt.Errorf("property must contain name")
	}
	prop.Name = schema.LowercaseFirstLetter(prop.Name)
	if prop.DataType == nil {
		return fmt.Errorf("property must contain dataType")
	}

	cls := m.metaReader.ReadOnlyClass(class)
	if cls == nil {
		return fmt.Errorf("class %q: %w", class, ErrNotFound)
	}

	existingPropertyNames := map[string]bool{}
	for _, existingProperty := range class.Properties {
		existingPropertyNames[strings.ToLower(existingProperty.Name)] = true
	}

	m.setNewPropDefaults(class, prop)
	if err := m.validateProperty(prop, class, existingPropertyNames, false); err != nil {
		return err
	}
	// migrate only after validation in completed
	migratePropertySettings(prop)
	return m.metaWriter.AddProperty(class, prop)
}

// DeleteClassProperty from existing Schema
func (m *Manager) DeleteClassProperty(ctx context.Context, principal *models.Principal,
	class string, property string,
) error {
	err := m.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	/// TODO-RAFT START
	/// Implement RAFT based DeleteClassProperty
	/// TODO-RAFT END

	return fmt.Errorf("deleting a property is currently not supported, see " +
		"https://github.com/weaviate/weaviate/issues/973 for details.")
	// return m.deleteClassProperty(ctx, class, property, kind.Action)
}

func (m *Handler) setNewPropDefaults(class *models.Class, prop *models.Property) {
	setPropertyDefaults(prop)
	m.moduleConfig.SetSinglePropertyDefaults(class, prop)
}

func (m *Handler) validatePropModuleConfig(class *models.Class, prop *models.Property) error {
	if prop.ModuleConfig == nil {
		return nil
	}
	modconfig, ok := prop.ModuleConfig.(map[string]interface{})
	if !ok {
		return fmt.Errorf("%v property config invalid", prop.Name)
	}

	if !hasTargetVectors(class) {
		configuredVectorizers := make([]string, 0, len(modconfig))
		for modName := range modconfig {
			if err := m.vectorizerValidator.ValidateVectorizer(modName); err == nil {
				configuredVectorizers = append(configuredVectorizers, modName)
			}
		}
		if len(configuredVectorizers) > 1 {
			return fmt.Errorf("multiple vectorizers configured in property's %q moduleConfig: %v. class.vectorizer is set to %q",
				prop.Name, configuredVectorizers, class.Vectorizer)
		}

		vectorizerConfig, ok := modconfig[class.Vectorizer]
		if !ok {
			if class.Vectorizer == "none" {
				return nil
			}
			return fmt.Errorf("%v vectorizer module not part of the property", class.Vectorizer)
		}
		_, ok = vectorizerConfig.(map[string]interface{})
		if !ok {
			return fmt.Errorf("vectorizer config for vectorizer %v, not of type map[string]interface{}", class.Vectorizer)
		}
		return nil
	}

	// TODO reuse for multiple props?
	vectorizersSet := map[string]struct{}{}
	for _, cfg := range class.VectorConfig {
		if vm, ok := cfg.Vectorizer.(map[string]interface{}); ok && len(vm) == 1 {
			for vectorizer := range vm {
				vectorizersSet[vectorizer] = struct{}{}
			}
		}
	}
	for vectorizer, cfg := range modconfig {
		if _, ok := vectorizersSet[vectorizer]; !ok {
			return fmt.Errorf("vectorizer %q not configured for any of target vectors", vectorizer)
		}
		if _, ok := cfg.(map[string]interface{}); !ok {
			return fmt.Errorf("vectorizer config for vectorizer %q not of type map[string]interface{}", vectorizer)
		}
	}

	return nil
}

// MergeClassObjectProperty of an existing Class
// Merges NestedProperties of incoming object/object[] property into existing one
func (m *Handler) MergeClassObjectProperty(ctx context.Context, principal *models.Principal,
	class string, property *models.Property,
) error {
	// TODO-RAFT
	return nil
	// 	err := m.Authorizer.Authorize(principal, "update", "schema/objects")
	// 	if err != nil {
	// 		return err
	// 	}

	// return m.mergeClassObjectProperty(ctx, class, property)
}

func (m *Handler) mergeClassObjectProperty(ctx context.Context,
	className string, prop *models.Property,
) error {
	return nil
	// m.Lock()
	// defer m.Unlock()

	// class, err := m.schemaCache.readOnlyClass(className)
	// if err != nil {
	// 	return err
	// }
	// prop.Name = schema.LowercaseFirstLetter(prop.Name)

	// // reuse setDefaults/validation/migrate methods coming from add property
	// // (empty existing names map, to validate existing updated property)
	// // TODO nested - refactor / cleanup setDefaults/validation/migrate methods
	// m.setNewPropDefaults(class, prop)
	// if err := m.validateProperty(prop, class, map[string]bool{}, false); err != nil {
	// 	return err
	// }
	// // migrate only after validation in completed
	// migratePropertySettings(prop)

	// tx, err := m.cluster.BeginTransaction(ctx, mergeObjectProperty,
	// 	MergeObjectPropertyPayload{className, prop}, DefaultTxTTL)
	// if err != nil {
	// 	// possible causes for errors could be nodes down (we expect every node to
	// 	// the up for a schema transaction) or concurrent transactions from other
	// 	// nodes
	// 	return errors.Wrap(err, "open cluster-wide transaction")
	// }

	// if err := m.cluster.CommitWriteTransaction(ctx, tx); err != nil {
	// 	// Only log the commit error, but do not abort the changes locally. Once
	// 	// we've told others to commit, we also need to commit ourselves!
	// 	//
	// 	// The idea is that if we abort our changes we are guaranteed to create an
	// 	// inconsistency as soon as any other node honored the commit. This would
	// 	// for example be the case in a 3-node cluster where node 1 is the
	// 	// coordinator, node 2 honored the commit and node 3 died during the commit
	// 	// phase.
	// 	//
	// 	// In this scenario it is far more desirable to make sure that node 1 and
	// 	// node 2 stay in sync, as node 3 - who may or may not have missed the
	// 	// update - can use a local WAL from the first TX phase to replay any
	// 	// missing changes once it's back.
	// 	m.logger.WithError(err).Errorf("not every node was able to commit")
	// }

	// return m.mergeClassObjectPropertyApplyChanges(ctx, className, prop)
}

func (m *Manager) mergeClassObjectPropertyApplyChanges(ctx context.Context,
	className string, prop *models.Property,
) error {
	// class, err := m.schemaCache.mergeObjectProperty(className, prop)
	// if err != nil {
	// 	return err
	// }
	// metadata, err := json.Marshal(&class)
	// if err != nil {
	// 	return fmt.Errorf("marshal class %s: %w", className, err)
	// }
	// m.logger.
	// 	WithField("action", "schema.update_object_property").
	// 	Debug("saving updated schema to configuration store")
	// err = m.repo.UpdateClass(ctx, ClassPayload{Name: className, Metadata: metadata})
	// if err != nil {
	// 	return err
	// }
	// m.triggerSchemaUpdateCallbacks()

	// // TODO nested - implement MergeObjectProperty (needed for indexing/filtering)
	// // will result in a mismatch between schema and index if function below fails
	// // return m.migrator.MergeObjectProperty(ctx, className, prop)
	return nil
}

func (m *Manager) deduplicateProps(props []*models.Property,
	className string,
) []*models.Property {
	seen := map[string]struct{}{}
	i := 0
	for j, prop := range props {
		name := strings.ToLower(prop.Name)
		if _, ok := seen[name]; ok {
			m.logger.WithFields(logrus.Fields{
				"action": "startup_repair_schema",
				"prop":   prop.Name,
				"class":  className,
			}).Warningf("removing duplicate property %s", prop.Name)
			continue
		}
		if i != j {
			props[i] = prop
		}
		seen[name] = struct{}{}
		i++
	}

	return props[:i]
}
