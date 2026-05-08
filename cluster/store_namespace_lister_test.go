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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

type failingSchemaSource struct {
	err error
}

func (f failingSchemaSource) ReadSchema(func(models.Class, uint64)) error { return f.err }
func (f failingSchemaSource) Aliases() map[string]string                  { return nil }

func TestSchemaNamespaceLister_ClassesInNamespace(t *testing.T) {
	src := fakeSchemaSource{classes: []string{
		"alpha:Foo",
		"alpha:Bar",
		"beta:Baz",
		"Unscoped",
	}}
	a := &SchemaNamespaceLister{src: src}

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
			got, err := a.ClassesInNamespace(tc.namespace)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSchemaNamespaceLister_ClassesInNamespace_ReadSchemaError(t *testing.T) {
	wantErr := errors.New("boom")
	a := &SchemaNamespaceLister{src: failingSchemaSource{err: wantErr}}

	got, err := a.ClassesInNamespace("alpha")
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, got)
}

func TestSchemaNamespaceLister_AliasesInNamespace(t *testing.T) {
	src := fakeSchemaSource{aliases: map[string]string{
		"alpha:A1": "alpha:Foo",
		"alpha:A2": "alpha:Bar",
		"beta:B1":  "beta:Baz",
		"Unscoped": "SomeClass",
	}}
	a := &SchemaNamespaceLister{src: src}

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
	a := &SchemaNamespaceLister{src: src}

	classes, err := a.ClassesInNamespace("alpha")
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha:Foo"}, classes)
	assert.ElementsMatch(t, []string{"alpha:A1"}, a.AliasesInNamespace("alpha"))
}
