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

package rest

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

// scopedAuthorizer grants a verb on exactly the resources it was built with
// and denies every other resource, which is what an RBAC role holding UPDATE
// on one collection looks like to a handler.
type scopedAuthorizer struct {
	granted map[string]bool
}

func grantUpdateOn(classes ...string) *scopedAuthorizer {
	granted := map[string]bool{}
	for _, resource := range authorization.Collections(classes...) {
		granted[resource] = true
	}
	return &scopedAuthorizer{granted: granted}
}

func (a *scopedAuthorizer) Authorize(ctx context.Context, principal *models.Principal,
	verb string, resources ...string,
) error {
	for _, resource := range resources {
		if !a.granted[resource] {
			return authzerrors.NewForbidden(principal, verb, resource)
		}
	}
	return nil
}

func (a *scopedAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal,
	verb string, resources ...string,
) error {
	return a.Authorize(ctx, principal, verb, resources...)
}

func (a *scopedAuthorizer) FilterAuthorizedResources(ctx context.Context, principal *models.Principal,
	verb string, resources ...string,
) ([]string, error) {
	var allowed []string
	for _, resource := range resources {
		if a.granted[resource] {
			allowed = append(allowed, resource)
		}
	}
	return allowed, nil
}

// Cancelling a task that names no collection is a cluster-wide act: it stops a
// migration on some other collection and answers with a task id that spells
// that collection and the migrated property out. The URL's own collection is
// the only thing updateIndex authorized, so this pass has to ask for more.
func TestCancelOfATaskThatNamesNoCollectionNeedsClusterWideUpdate(t *testing.T) {
	const (
		collection = "Movies"
		foreignID  = "Reviews:change_tokenization:body:ab3f"
	)

	tests := []struct {
		name          string
		authorizer    authorization.Authorizer
		wantStatus    string
		wantCancelled bool
	}{
		{
			name:          "UPDATE on every collection cancels it",
			authorizer:    grantUpdateOn(),
			wantStatus:    "CANCELLED",
			wantCancelled: true,
		},
		{
			name:       "UPDATE on the URL's collection alone is not enough",
			authorizer: grantUpdateOn(collection),
			// The same answer the caller would get if this pass did not exist,
			// so a denied caller cannot tell the foreign task apart from none.
			wantStatus: reindexCancelStatusNoOp,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &raceTaskService{tasks: []*distributedtask.Task{
				unattributableTask(foreignID, distributedtask.TaskStatusStarted),
			}}
			var busy atomic.Bool
			h := submissionHandlers(t, svc, togglingProber{busy: &busy})
			h.appState.Authorizer = tc.authorizer
			h.appState.ReindexProvider.Store(db.NewReindexProvider(nil, nil, h.appState.Logger, fixtureNode,
				func() int { return 1 }, context.Background()))

			responder := h.cancelReindexTask(context.Background(), collection, "title", "filterable",
				&models.Principal{Username: "u1"})

			accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
			require.Truef(t, ok, "cancel must be accepted, got %T", responder)
			require.Equal(t, tc.wantStatus, accepted.Payload.Status)

			if !tc.wantCancelled {
				require.Empty(t, svc.cancelled,
					"a caller without cluster-wide UPDATE must not stop a migration it has no grant for")
				require.NotContains(t, strings.ToLower(accepted.Payload.TaskID), "reviews",
					"the response must not name the foreign collection")
				return
			}
			require.Len(t, svc.cancelled, 1)
			require.Equal(t, foreignID, svc.cancelled[0].ID)
			require.Equal(t, foreignID, accepted.Payload.TaskID,
				"a cluster-privileged caller gets the full id, which is the handle on the task")
		})
	}
}
