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

package v1

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/schema"
)

// existsOnlyRepo answers HeadObject's single repo call. The embedded interface
// is nil, so any other method panics rather than returning a value a row could
// pass on. usecases/objects has no generated VectorRepo mock.
type existsOnlyRepo struct {
	objects.VectorRepo
}

func (existsOnlyRepo) Exists(ctx context.Context, class string, id strfmt.UUID,
	repl *additional.ReplicationProperties, tenant string,
) (bool, error) {
	return true, nil
}

// TestHeadObjectRequiresActiveNamespace drives the real objects.Manager through
// the real rbac.Manager and the real namespace controller, so a read that never
// reaches the gate fails here. One refused state is enough for that:
// usecases/namespaces.RequireActive owns the state-to-sentinel mapping. It lives
// here rather than in usecases/objects because a package objects test file
// cannot import usecases/namespaces without an import cycle.
func TestHeadObjectRequiresActiveNamespace(t *testing.T) {
	tests := []struct {
		name        string
		transitions []cmd.NamespaceState
		wantErr     error
	}{
		{
			name: "an active namespace is served",
		},
		{
			name:        "a suspended namespace is refused",
			transitions: []cmd.NamespaceState{cmd.NamespaceStateSuspended},
			wantErr:     usecasesNamespaces.ErrNamespaceSuspended,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			reader := schema.NewMockSchemaReader(t)
			reader.On("ReadOnlyClass", gatedClass).Return(&models.Class{Class: gatedClass}).Maybe()
			reader.On("ResolveAlias", gatedClass).Return("").Maybe()

			cfg := &config.WeaviateConfig{Config: config.Config{
				Namespaces: config.Namespaces{Enabled: true},
			}}
			manager := objects.NewManager(
				&schema.Manager{SchemaReader: reader}, cfg, logger,
				namespaceAwareRBAC(t, namespaceIn(t, tt.transitions)),
				existsOnlyRepo{}, nil, objects.NewMetrics(nil), nil, nil)

			_, err := manager.HeadObject(context.Background(), gateRootPrincipal(),
				gatedClass, strfmt.UUID("d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"), nil, "")

			if tt.wantErr != nil {
				require.NotNil(t, err)
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.Nil(t, err)
		})
	}
}
