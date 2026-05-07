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

	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

// SchemaSource is the subset of [schema.SchemaReader] consumed by
// [SchemaNamespaceLister]; defined as an interface so tests can stub it.
type SchemaSource interface {
	ReadSchema(reader func(models.Class, uint64)) error
	Aliases() map[string]string
}

// SchemaNamespaceLister returns the classes and aliases whose name starts
// with "<namespace>:".
type SchemaNamespaceLister struct {
	src SchemaSource
}

func NewSchemaNamespaceLister(src SchemaSource) *SchemaNamespaceLister {
	return &SchemaNamespaceLister{src: src}
}

func (a *SchemaNamespaceLister) ClassesInNamespace(namespace string) []string {
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

func (a *SchemaNamespaceLister) AliasesInNamespace(namespace string) []string {
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
