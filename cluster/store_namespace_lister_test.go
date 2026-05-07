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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/models"
)

type fakeSchemaSource struct {
	classes []string
	aliases map[string]string
}

func (f fakeSchemaSource) ReadSchema(reader func(models.Class, uint64)) error {
	for _, name := range f.classes {
		reader(models.Class{Class: name}, 0)
	}
	return nil
}

func (f fakeSchemaSource) Aliases() map[string]string { return f.aliases }

func TestSchemaNamespaceLister_ClassesInNamespace(t *testing.T) {
	src := fakeSchemaSource{classes: []string{
		"alpha:Foo",
		"alpha:Bar",
		"beta:Baz",
		"Unscoped",
	}}
	a := &schemaNamespaceLister{src: src}

	tests := []struct {
		name      string
		namespace string
		want      []string
	}{
		{name: "namespace with multiple classes", namespace: "alpha", want: []string{"alpha:Foo", "alpha:Bar"}},
		{name: "namespace with one class", namespace: "beta", want: []string{"beta:Baz"}},
		{name: "missing namespace returns empty", namespace: "ghost", want: []string{}},
		{name: "empty namespace returns nil", namespace: "", want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, a.ClassesInNamespace(tc.namespace))
		})
	}
}

func TestSchemaNamespaceLister_AliasesInNamespace(t *testing.T) {
	src := fakeSchemaSource{aliases: map[string]string{
		"alpha:A1": "alpha:Foo",
		"alpha:A2": "alpha:Bar",
		"beta:B1":  "beta:Baz",
		"Unscoped": "SomeClass",
	}}
	a := &schemaNamespaceLister{src: src}

	tests := []struct {
		name      string
		namespace string
		want      []string
	}{
		{name: "namespace with multiple aliases", namespace: "alpha", want: []string{"alpha:A1", "alpha:A2"}},
		{name: "namespace with one alias", namespace: "beta", want: []string{"beta:B1"}},
		{name: "missing namespace returns empty", namespace: "ghost", want: []string{}},
		{name: "empty namespace returns nil", namespace: "", want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, a.AliasesInNamespace(tc.namespace))
		})
	}
}

func TestSchemaNamespaceLister_PrefixIsExact(t *testing.T) {
	src := fakeSchemaSource{
		classes: []string{"alpha:Foo", "alphabet:Bar"},
		aliases: map[string]string{"alpha:A1": "alpha:Foo", "alphabet:B1": "alphabet:Bar"},
	}
	a := &schemaNamespaceLister{src: src}

	assert.Equal(t, []string{"alpha:Foo"}, a.ClassesInNamespace("alpha"))
	assert.ElementsMatch(t, []string{"alpha:A1"}, a.AliasesInNamespace("alpha"))
}
