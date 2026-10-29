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

package schema

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// TestQualifierRefusalKeepsItsChain pins that each Handler method below wraps a
// qualifier refusal with %w. Its REST handler answers 403 only while errors.As
// still finds the Forbidden.
func TestQualifierRefusalKeepsItsChain(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	refusal := fmt.Errorf("refused: %w", authzerrors.NewForbidden(nil, "read", "collections/Movies"))

	calls := []struct {
		name string
		call func(h *Handler, principal *models.Principal) error
	}{
		{name: "AddAlias", call: func(h *Handler, principal *models.Principal) error {
			_, _, err := h.AddAlias(ctx, principal, &models.Alias{Alias: "Films", Class: "Movies"})
			return err
		}},
		{name: "ShardsStatus", call: func(h *Handler, principal *models.Principal) error {
			_, err := h.ShardsStatus(ctx, principal, "Movies", "shard1")
			return err
		}},
		{name: "UpdateShardStatus", call: func(h *Handler, principal *models.Principal) error {
			_, err := h.UpdateShardStatus(ctx, principal, "Movies", "shard1", "READY")
			return err
		}},
	}
	principals := map[string]*models.Principal{
		"global":     globalPrincipal(),
		"namespaced": namespacedPrincipal("customer1"),
	}

	for _, c := range calls {
		for pName, principal := range principals {
			t.Run(c.name+"/"+pName, func(t *testing.T) {
				t.Parallel()
				handler, _ := newTestHandlerWithNamespaces(t, true)
				handler.qualifier = namespacing.Refusing(refusal)

				require.ErrorIs(t, c.call(handler, principal), refusal)
			})
		}
	}
}
