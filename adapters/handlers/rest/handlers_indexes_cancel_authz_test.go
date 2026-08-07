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
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
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

// auditingAuthorizer records denials the way the RBAC authorizer does: the
// auditing entry point files an ERROR-level "authorization denied", the silent
// one files nothing. Which of the two a call site picks is visible only here.
type auditingAuthorizer struct {
	*scopedAuthorizer
	logger logrus.FieldLogger
}

func (a auditingAuthorizer) Authorize(ctx context.Context, principal *models.Principal,
	verb string, resources ...string,
) error {
	err := a.scopedAuthorizer.Authorize(ctx, principal, verb, resources...)
	if err != nil {
		a.logger.WithField("resource", resources).Error("authorization denied")
	}
	return err
}

// The cluster-wide check on the unattributable-task pass is a capability probe,
// not something the caller asked for: an ordinary single-collection cancel that
// merely coincides with such a task is answered 202 and must not leave a
// security-alert-shaped record behind for a request that succeeded.
func TestCancelCapabilityProbeFilesNoAuthorizationDenial(t *testing.T) {
	const (
		collection = "Movies"
		foreignID  = "Reviews:change_tokenization:body:ab3f"
	)

	svc := &raceTaskService{tasks: []*distributedtask.Task{
		unattributableTask(foreignID, distributedtask.TaskStatusStarted),
	}}
	var busy atomic.Bool
	h := submissionHandlers(t, svc, togglingProber{busy: &busy})

	logger, hook := logrustest.NewNullLogger()
	h.appState.Logger = logger
	h.appState.Authorizer = auditingAuthorizer{scopedAuthorizer: grantUpdateOn(collection), logger: logger}
	h.appState.ReindexProvider.Store(db.NewReindexProvider(nil, nil, h.appState.Logger, fixtureNode,
		func() int { return 1 }, context.Background()))

	responder := h.cancelReindexTask(context.Background(), collection, "title", "filterable",
		&models.Principal{Username: "u1"})

	accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
	require.Truef(t, ok, "cancel must be accepted, got %T", responder)
	require.Equal(t, reindexCancelStatusNoOp, accepted.Payload.Status)

	for _, entry := range hook.AllEntries() {
		require.NotEqualf(t, logrus.ErrorLevel, entry.Level,
			"a request answered %q must not file an error-level audit record: %q",
			accepted.Payload.Status, entry.Message)
	}
	require.Contains(t, entryMessages(hook), "reindex_task_cancel_unattributable_denied",
		"the handler still has to record the denial itself, at its own level")
}

// entryMessages joins each entry's message with its audit_event field, so a
// test can assert on either without knowing which carries the text.
func entryMessages(hook *logrustest.Hook) string {
	var b strings.Builder
	for _, entry := range hook.AllEntries() {
		b.WriteString(entry.Message)
		if event, ok := entry.Data["audit_event"]; ok {
			fmt.Fprint(&b, " ", event)
		}
		b.WriteString("\n")
	}
	return b.String()
}
