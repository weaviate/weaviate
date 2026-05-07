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

package cluster

import (
	"strings"

	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

// schemaSource is the subset of [schema.SchemaReader] used here; it is an
// interface so tests can substitute a stub.
type schemaSource interface {
	ReadSchema(reader func(models.Class, uint64)) error
	Aliases() map[string]string
}

// schemaNamespaceLister returns the classes and aliases whose name starts
// with "<namespace>:".
type schemaNamespaceLister struct {
	src schemaSource
}

func newSchemaNamespaceLister(manager *schema.SchemaManager) *schemaNamespaceLister {
	return &schemaNamespaceLister{src: manager.NewSchemaReader()}
}

func (a *schemaNamespaceLister) ClassesInNamespace(namespace string) []string {
	if namespace == "" {
		return nil
	}
	prefix := namespace + entschema.NamespaceSeparator
	out := make([]string, 0)
	_ = a.src.ReadSchema(func(class models.Class, _ uint64) {
		if strings.HasPrefix(class.Class, prefix) {
			out = append(out, class.Class)
		}
	})
	return out
}

func (a *schemaNamespaceLister) AliasesInNamespace(namespace string) []string {
	if namespace == "" {
		return nil
	}
	prefix := namespace + entschema.NamespaceSeparator
	out := make([]string, 0)
	for alias := range a.src.Aliases() {
		if strings.HasPrefix(alias, prefix) {
			out = append(out, alias)
		}
	}
	return out
}
