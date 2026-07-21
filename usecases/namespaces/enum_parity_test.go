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

package namespaces

import (
	"testing"

	"github.com/stretchr/testify/assert"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
)

// The server emits cmd.NamespaceState* strings into models.Namespace.State,
// while the generated schema.json enum (mirrored by models.NamespaceState*)
// is the wire contract third-party clients validate against. Nothing else
// links the two constant sets: this table fails to compile if a swagger
// regen drops or renames a models enum value, and fails at runtime if the
// two sides ever disagree on the string.
func TestNamespaceStateEnumParity(t *testing.T) {
	tests := []struct {
		name    string
		control cmd.NamespaceState
		model   string
	}{
		{name: "active", control: cmd.NamespaceStateActive, model: models.NamespaceStateActive},
		{name: "suspended", control: cmd.NamespaceStateSuspended, model: models.NamespaceStateSuspended},
		{name: "resuming", control: cmd.NamespaceStateResuming, model: models.NamespaceStateResuming},
		{name: "deleting", control: cmd.NamespaceStateDeleting, model: models.NamespaceStateDeleting},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.model, string(tt.control))
		})
	}
}
