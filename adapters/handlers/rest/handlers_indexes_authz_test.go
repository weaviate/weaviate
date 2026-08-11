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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

// recordingSubmitAuthorizer answers every check with a fixed verdict and
// remembers what it was asked, so a test can tell a refusal from a check that
// never happened.
type recordingSubmitAuthorizer struct {
	err error

	verbs     []string
	resources [][]string
}

func (a *recordingSubmitAuthorizer) Authorize(_ context.Context, _ *models.Principal,
	verb string, resources ...string,
) error {
	a.verbs = append(a.verbs, verb)
	a.resources = append(a.resources, resources)
	return a.err
}

func (a *recordingSubmitAuthorizer) AuthorizeSilent(ctx context.Context, pr *models.Principal,
	verb string, resources ...string,
) error {
	return a.Authorize(ctx, pr, verb, resources...)
}

func (a *recordingSubmitAuthorizer) FilterAuthorizedResources(_ context.Context, _ *models.Principal,
	_ string, resources ...string,
) ([]string, error) {
	if a.err != nil {
		return nil, a.err
	}
	return resources, nil
}

// authzSubmitFixture wires the submission handler so every collaborator
// behind the authorization check records that it ran. The on-disk sweep has
// no fixture seam, so it's covered by the fan-out probe, which strictly
// precedes it under the same gate.
type authzSubmitFixture struct {
	handlers *indexesHandlers
	authz    *recordingSubmitAuthorizer
	tasks    *raceTaskService
	local    *localSlotProbe
	fanOut   *gateObservingProber
}

func newAuthzSubmitFixture(t *testing.T, authzErr error) *authzSubmitFixture {
	t.Helper()

	svc := &raceTaskService{}
	h, provider := gatePriorityHandlers(t, svc)

	authz := &recordingSubmitAuthorizer{err: authzErr}
	h.appState.Authorizer = authz

	local := &localSlotProbe{provider: provider}
	h.localBackupActivity = local
	fanOut := &gateObservingProber{provider: provider}
	h.backupActivity = fanOut

	return &authzSubmitFixture{handlers: h, authz: authz, tasks: svc, local: local, fanOut: fanOut}
}

// requireNothingBehindTheCheckRan states the property a status-code assertion
// cannot: the handler stopped AT the check, rather than doing the work and
// refusing afterwards.
func (f *authzSubmitFixture) requireNothingBehindTheCheckRan(t *testing.T) {
	t.Helper()

	require.Emptyf(t, f.local.observed(),
		"this node's own backup slots were read for a caller that has no permission to submit")
	require.Emptyf(t, f.fanOut.observed(),
		"a refused caller triggered the cluster-wide backup fan-out; the gate was closed on the "+
			"collection and the destructive stale-state sweep behind it became reachable")
	require.Zerof(t, f.tasks.lists,
		"a refused caller made the handler read the cluster's task list")
	require.Zerof(t, f.tasks.adds,
		"a refused caller got a reindex task written to RAFT")
}

// The submit route is the privileged arm of PUT .../indexes/{prop}: behind its
// authorization check the handler closes the collection's backup gate, fans a
// probe out over every node, and deletes stale on-disk reindex state. A
// regression in the check does not just leak an answer — it hands an
// unauthorized caller all three.
func TestUpdateIndexAuthorization(t *testing.T) {
	principal := &models.Principal{Username: "u1"}

	// Real authorizers wrap the denial (e.g. "rbac:" prefix); a bare denial
	// alone would keep this arm green while production refusals came back 500.
	denials := []struct {
		name string
		err  func() error
	}{
		{"a bare denial", func() error {
			return authzerrors.NewForbidden(principal, authorization.UPDATE,
				authorization.Collections("Movies")...)
		}},
		{"a denial wrapped the way the real authorizers wrap it", func() error {
			return forbidden(principal, authorization.UPDATE, authorization.Collections("Movies")[0])
		}},
	}

	for _, denial := range denials {
		t.Run("an unauthorized caller is refused before anything behind the check runs: "+denial.name, func(t *testing.T) {
			denied := denial.err()
			f := newAuthzSubmitFixture(t, denied)

			responder := submitReindex(f.handlers)

			// Asserted before the status code on purpose. A refusal handed back
			// after the work already happened carries the same 403, so checking
			// the code first would let that regression abort the test here and
			// never reach the assertions that would have caught it.
			f.requireNothingBehindTheCheckRan(t)

			refused, ok := responder.(*schema.SchemaObjectsIndexesUpdateForbidden)
			require.Truef(t, ok, "a caller without update_collections must be refused with 403, got %T", responder)
			require.Equal(t, denied.Error(), errorMessage(t, refused.Payload))
		})
	}

	// A check that fails for a reason other than "denied" must not be read as a
	// grant, and must stop the handler in the same place.
	t.Run("an authorizer that errors refuses too, and just as early", func(t *testing.T) {
		f := newAuthzSubmitFixture(t, errors.New("policy store unreachable"))

		responder := submitReindex(f.handlers)

		f.requireNothingBehindTheCheckRan(t)

		failed, ok := responder.(*schema.SchemaObjectsIndexesUpdateInternalServerError)
		require.Truef(t, ok, "an authorizer that cannot answer must not admit the submission, got %T", responder)
		require.Equal(t, "policy store unreachable", errorMessage(t, failed.Payload))
	})

	// The allow arm is what makes the two arms above discriminate: it proves the
	// same observers do fire when the check passes, so their emptiness on the
	// deny arms is the check's doing and not the fixture's.
	t.Run("an authorized caller reaches every step behind the check", func(t *testing.T) {
		f := newAuthzSubmitFixture(t, nil)

		responder := submitReindex(f.handlers)

		_, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
		require.Truef(t, ok, "a caller holding update_collections must be admitted, got %T", responder)

		require.Equal(t, []db.ReindexHold{db.ReindexHoldNone}, f.local.observed(),
			"the local slots are read once, ahead of the gate")
		require.Equal(t, []db.ReindexHold{db.ReindexHoldSubmit, db.ReindexHoldSubmit}, f.fanOut.observed(),
			"the fan-out runs once before and once after the commit, both under the closed gate; "+
				"this is the observation the deny arms require to be absent")
		require.Equal(t, 1, f.tasks.lists)
		require.Equal(t, 1, f.tasks.adds)
	})

	// The verb and resource are the check itself. Weakening either — to a read
	// verb, or to the metadata resource the sibling GET route uses — still
	// refuses some callers, so the arms above would keep passing.
	t.Run("the check demands UPDATE on the collection", func(t *testing.T) {
		f := newAuthzSubmitFixture(t, nil)

		submitReindex(f.handlers)

		require.Equal(t, []string{authorization.UPDATE}, f.authz.verbs,
			"submitting rebuilds buckets on every replica and flips schema flags; "+
				"a read verb is not enough to authorize it")
		require.Equal(t, [][]string{authorization.Collections("Movies")}, f.authz.resources,
			"the check must be scoped to the collection being reindexed")
	})
}

// The status route is the read arm of GET .../indexes: behind its
// authorization check the handler reads the collection's schema and the
// cluster's task list. The check is all that keeps per-property index state
// — collection-internal information — from an unauthorized caller.
func TestGetIndexesAuthorization(t *testing.T) {
	principal := &models.Principal{Username: "u1"}

	// The denial arrives wrapped, the way both real authorizers wrap it
	// ("rbac:" / "adminlist:" prefix). The bare form cannot fail on its own:
	// any unwrapping regression reddens this arm first. The sibling PUT test
	// keeps the bare-vs-wrapped pair as the documentary row for the trap.
	t.Run("an unauthorized caller is refused before anything behind the check runs", func(t *testing.T) {
		denied := forbidden(principal, authorization.READ, authorization.CollectionsMetadata("Movies")[0])
		f := newAuthzSubmitFixture(t, denied)

		responder := getIndexesStatus(f.handlers)

		// The task-list read is the observable work behind this route's
		// check; the allow arm below proves it does fire when admitted.
		require.Zerof(t, f.tasks.lists,
			"a refused caller made the handler read the cluster's task list")

		refused, ok := responder.(*schema.SchemaObjectsIndexesGetForbidden)
		require.Truef(t, ok, "a caller without read_collections must be refused with 403, got %T", responder)
		require.Equal(t, denied.Error(), errorMessage(t, refused.Payload))
	})

	// A check that fails for a reason other than "denied" must not be read as a
	// grant, and must stop the handler in the same place.
	t.Run("an authorizer that errors refuses too, and just as early", func(t *testing.T) {
		f := newAuthzSubmitFixture(t, errors.New("policy store unreachable"))

		responder := getIndexesStatus(f.handlers)

		require.Zerof(t, f.tasks.lists,
			"an authorizer that cannot answer let the handler read the cluster's task list")

		failed, ok := responder.(*schema.SchemaObjectsIndexesGetInternalServerError)
		require.Truef(t, ok, "an authorizer that cannot answer must not admit the read, got %T", responder)
		require.Equal(t, "policy store unreachable", errorMessage(t, failed.Payload))
	})

	// The allow arm is what makes the arms above discriminate: it proves the
	// task list is read when the check passes, so its absence on the deny arms
	// is the check's doing and not the fixture's.
	t.Run("an authorized caller gets the index state behind the check", func(t *testing.T) {
		f := newAuthzSubmitFixture(t, nil)

		responder := getIndexesStatus(f.handlers)

		granted, ok := responder.(*schema.SchemaObjectsIndexesGetOK)
		require.Truef(t, ok, "a caller holding read_collections must be answered 200, got %T", responder)
		require.Equal(t, "Movies", granted.Payload.Collection)
		require.NotEmpty(t, granted.Payload.Properties,
			"the answer carries the per-property index state the check guards")
		require.Equal(t, 1, f.tasks.lists,
			"the task list is read once, behind the check; this is the observation the deny arms require to be absent")
	})

	// The verb and resource are the check itself. Strengthening the resource to
	// the full collection (what the sibling PUT uses) would refuse readers who
	// legitimately hold only metadata access, and a write verb would do the same.
	t.Run("the check demands READ on the collection's metadata", func(t *testing.T) {
		f := newAuthzSubmitFixture(t, nil)

		getIndexesStatus(f.handlers)

		require.Equal(t, []string{authorization.READ}, f.authz.verbs,
			"the route only reads index state; it must not demand more than READ")
		require.Equal(t, [][]string{authorization.CollectionsMetadata("Movies")}, f.authz.resources,
			"the check must be scoped to the collection's metadata, the resource that state belongs to")
	})
}
