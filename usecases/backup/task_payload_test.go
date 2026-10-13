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

package backup

import (
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/backup"
)

// Every taskPayload field belongs to exactly one of these lists.
var (
	requestKeyFields = []string{
		"Backend", "Classes", "Users", "Roles", "RoleNamespaces", "SkipUsers", "SkipRoles",
		"Compression", "Bucket", "Path", "BaseBackupID", "DedupeReplicas", "DedupeConvergenceTimeoutSeconds",
	}
	derivedFields = []string{
		"Nodes", "Leader", "ServerVersion", "CompressionType", "BaseChainDeduped", "BuiltInRoleAssignments",
	}
	idFields = []string{"ID"}
)

func TestTaskPayloadFields(t *testing.T) {
	t.Run("every field is a request key, derived, or the ID", func(t *testing.T) {
		lists := [][]string{requestKeyFields, derivedFields, idFields}
		var names []string
		for _, f := range reflect.VisibleFields(reflect.TypeFor[taskPayload]()) {
			names = append(names, f.Name)
			in := 0
			for _, list := range lists {
				if slices.Contains(list, f.Name) {
					in++
				}
			}
			assert.Equal(t, 1, in, "field %s must be in exactly one of requestKeyFields, derivedFields, idFields", f.Name)
		}
		for _, name := range slices.Concat(lists...) {
			assert.Contains(t, names, name, "listed field %s is not a taskPayload field", name)
		}
	})
}

func TestSameRequestConfig(t *testing.T) {
	// full sets every field to a non-zero value, so each row's mutation is a
	// real change.
	full := func() *taskPayload {
		return &taskPayload{
			ID:                              "b1",
			Backend:                         "s3",
			Nodes:                           map[string]*backup.NodeDescriptor{"node-1": {Classes: []string{"Article", "Book"}}},
			Leader:                          "node-1",
			Classes:                         []string{"Article", "Book"},
			Users:                           []string{"ns1:alice"},
			Roles:                           []string{"ns1:reader"},
			RoleNamespaces:                  []string{"ns1"},
			SkipUsers:                       true,
			SkipRoles:                       true,
			Compression:                     Compression{Level: GzipBestSpeed, CPUPercentage: 50},
			Bucket:                          "bucket",
			Path:                            "path",
			BaseBackupID:                    "base",
			ServerVersion:                   "1.30.0",
			CompressionType:                 backup.CompressionGZIP,
			BaseChainDeduped:                true,
			DedupeReplicas:                  true,
			DedupeConvergenceTimeoutSeconds: 30,
			BuiltInRoleAssignments:          []byte(`{"version":1}`),
		}
	}
	tests := []struct {
		field  string
		mutate func(p *taskPayload)
	}{
		{"Backend", func(p *taskPayload) { p.Backend = "gcs" }},
		{"Classes", func(p *taskPayload) { p.Classes = []string{"Article", "Movie"} }},
		{"Users", func(p *taskPayload) { p.Users = []string{"ns1:bob"} }},
		{"Roles", func(p *taskPayload) { p.Roles = []string{"ns1:writer"} }},
		{"RoleNamespaces", func(p *taskPayload) { p.RoleNamespaces = []string{"ns1", "ns2"} }},
		{"SkipUsers", func(p *taskPayload) { p.SkipUsers = false }},
		{"SkipRoles", func(p *taskPayload) { p.SkipRoles = false }},
		{"Compression", func(p *taskPayload) { p.Compression.Level = GzipBestCompression }},
		{"Bucket", func(p *taskPayload) { p.Bucket = "other-bucket" }},
		{"Path", func(p *taskPayload) { p.Path = "other-path" }},
		{"BaseBackupID", func(p *taskPayload) { p.BaseBackupID = "other-base" }},
		{"DedupeReplicas", func(p *taskPayload) { p.DedupeReplicas = false }},
		{"DedupeConvergenceTimeoutSeconds", func(p *taskPayload) { p.DedupeConvergenceTimeoutSeconds = 60 }},
	}

	t.Run("identical payloads are the same request", func(t *testing.T) {
		assert.True(t, full().sameRequestConfig(full()))
	})

	t.Run("the rows cover every request-key field", func(t *testing.T) {
		var fields []string
		for _, tc := range tests {
			fields = append(fields, tc.field)
		}
		assert.ElementsMatch(t, requestKeyFields, fields)
	})

	for _, tc := range tests {
		t.Run(tc.field+" drift is a different request", func(t *testing.T) {
			stored, retried := full(), full()
			tc.mutate(stored)

			sv, rv := reflect.ValueOf(stored).Elem(), reflect.ValueOf(retried).Elem()
			for i := range sv.NumField() {
				name := sv.Type().Field(i).Name
				differs := !reflect.DeepEqual(sv.Field(i).Interface(), rv.Field(i).Interface())
				assert.Equal(t, name == tc.field, differs, "the row must change %s and nothing else; field %s", tc.field, name)
			}
			assert.False(t, retried.sameRequestConfig(stored))
		})
	}
}
