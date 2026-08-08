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
	"net/http"
	"testing"

	"github.com/go-openapi/runtime/middleware"
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

	t.Run("an unauthorized caller is refused before anything behind the check runs", func(t *testing.T) {
		forbidden := authzerrors.NewForbidden(principal, authorization.UPDATE,
			authorization.Collections("Movies")...)
		f := newAuthzSubmitFixture(t, forbidden)

		responder := submitReindex(f.handlers)

		// Asserted before the status code on purpose. A refusal handed back
		// after the work already happened carries the same 403, so checking
		// the code first would let that regression abort the test here and
		// never reach the assertions that would have caught it.
		f.requireNothingBehindTheCheckRan(t)

		refused, ok := responder.(*schema.SchemaObjectsIndexesUpdateForbidden)
		require.Truef(t, ok, "a caller without update_collections must be refused with 403, got %T", responder)
		require.Equal(t, forbidden.Error(), errorMessage(t, refused.Payload))
	})

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

	// Every arm above names a collection the fixture registers, so a handler
	// that answered 404 first would still refuse them with 403. This one names
	// a collection that does not exist: under the check-after-lookup ordering
	// it comes back 404, which tells an unauthorized caller which collections
	// are real, and it takes the per-property submit lock on the way there.
	t.Run("a refused caller cannot tell an absent collection from a present one", func(t *testing.T) {
		forbidden := authzerrors.NewForbidden(principal, authorization.UPDATE,
			authorization.Collections("Absent")...)
		f := newAuthzSubmitFixture(t, forbidden)

		responder := submitReindexForClass(f.handlers, context.Background(), "Absent")

		f.requireNothingBehindTheCheckRan(t)
		_, ok := responder.(*schema.SchemaObjectsIndexesUpdateForbidden)
		require.Truef(t, ok,
			"authorization must run before the collection lookup, so both answers are 403, got %T",
			responder)
	})
}

// getIndexesResponder drives GET /v1/schema/Movies/indexes and hands back the
// raw responder, so a deny arm can assert on the type rather than on a payload
// that only exists when the read was allowed.
func getIndexesResponder(t *testing.T, h *indexesHandlers, principal *models.Principal) middleware.Responder {
	t.Helper()
	return getIndexesResponderFor(t, h, principal, "Movies")
}

func getIndexesResponderFor(t *testing.T, h *indexesHandlers, principal *models.Principal, collection string) middleware.Responder {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "/v1/schema/"+collection+"/indexes", nil)
	require.NoError(t, err)
	return h.getIndexes(schema.SchemaObjectsIndexesGetParams{
		HTTPRequest: req,
		ClassName:   collection,
	}, principal)
}

// The status route exposes per-property index state, which is
// collection-internal. Deleting its whole authorization block leaves the rest
// of this package green, so these arms are the only thing standing between the
// route and an unauthorized reader.
func TestGetIndexesAuthorization(t *testing.T) {
	principal := &models.Principal{Username: "u1"}

	newFixture := func(t *testing.T, authzErr error) (*indexesHandlers, *recordingSubmitAuthorizer, *raceTaskService) {
		t.Helper()
		svc := &raceTaskService{}
		h, _ := gatePriorityHandlers(t, svc)
		authz := &recordingSubmitAuthorizer{err: authzErr}
		h.appState.Authorizer = authz
		return h, authz, svc
	}

	t.Run("an unauthorized caller is refused before the task list is read", func(t *testing.T) {
		forbidden := authzerrors.NewForbidden(principal, authorization.READ,
			authorization.CollectionsMetadata("Movies")...)
		h, _, svc := newFixture(t, forbidden)

		responder := getIndexesResponder(t, h, principal)

		require.Zerof(t, svc.lists,
			"a refused caller made the handler read the cluster's task list")

		refused, ok := responder.(*schema.SchemaObjectsIndexesGetForbidden)
		require.Truef(t, ok, "a caller without read_collections must be refused with 403, got %T", responder)
		require.Equal(t, forbidden.Error(), errorMessage(t, refused.Payload))
	})

	t.Run("an authorizer that errors refuses too, and just as early", func(t *testing.T) {
		h, _, svc := newFixture(t, errors.New("policy store unreachable"))

		responder := getIndexesResponder(t, h, principal)

		require.Zerof(t, svc.lists, "an authorizer that cannot answer must not admit the read")
		_, ok := responder.(*schema.SchemaObjectsIndexesGetInternalServerError)
		require.Truef(t, ok, "an unanswerable authorizer must not be read as a grant, got %T", responder)
	})

	// The allow arm is what makes the two above discriminate: it proves the task
	// read does happen when the check passes.
	t.Run("an authorized caller reaches the task read", func(t *testing.T) {
		h, _, svc := newFixture(t, nil)

		responder := getIndexesResponder(t, h, principal)

		_, ok := responder.(*schema.SchemaObjectsIndexesGetOK)
		require.Truef(t, ok, "a caller holding read_collections must be answered, got %T", responder)
		require.Equal(t, 1, svc.lists,
			"this is the observation the deny arms require to be absent")
	})

	// Weakening the verb or the resource still refuses some callers, so the arms
	// above would keep passing.
	t.Run("the check demands READ on the collection's metadata", func(t *testing.T) {
		h, authz, _ := newFixture(t, nil)

		getIndexesResponder(t, h, principal)

		require.Equal(t, []string{authorization.READ}, authz.verbs)
		require.Equal(t, [][]string{authorization.CollectionsMetadata("Movies")}, authz.resources,
			"the check must be scoped to the collection whose index state is exposed")
	})

	// The absent-collection arm, same reasoning as the submit route's.
	t.Run("a refused caller cannot tell an absent collection from a present one", func(t *testing.T) {
		forbidden := authzerrors.NewForbidden(principal, authorization.READ,
			authorization.CollectionsMetadata("Absent")...)
		h, _, svc := newFixture(t, forbidden)

		responder := getIndexesResponderFor(t, h, principal, "Absent")

		require.Zerof(t, svc.lists,
			"a refused caller made the handler read the cluster's task list")

		_, ok := responder.(*schema.SchemaObjectsIndexesGetForbidden)
		require.Truef(t, ok,
			"authorization must run before the collection lookup, so both answers are 403, got %T",
			responder)
	})
}
