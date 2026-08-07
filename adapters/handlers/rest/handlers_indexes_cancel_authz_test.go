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

// auditingAuthorizer records the way the RBAC authorizer does: the auditing
// entry point files an ERROR-level "authorization denied" for a refusal and one
// grant record for an allow, the silent one files neither. Which of the two a
// call site picks is visible only here.
type auditingAuthorizer struct {
	*scopedAuthorizer
	logger logrus.FieldLogger
}

const auditGranted = "authorization granted"

func (a auditingAuthorizer) Authorize(ctx context.Context, principal *models.Principal,
	verb string, resources ...string,
) error {
	err := a.scopedAuthorizer.Authorize(ctx, principal, verb, resources...)
	if err != nil {
		a.logger.WithField("resource", resources).Error("authorization denied")
		return err
	}
	a.logger.WithFields(logrus.Fields{"verb": verb, "resource": resources}).Info(auditGranted)
	return nil
}

// outageAuthorizer cannot answer at all. Its error is not a Forbidden, so the
// caller's grant is unknown rather than absent — the one case where refusing
// tells the operator nothing about the caller.
type outageAuthorizer struct{}

var errAuthorizerUnavailable = errors.New("policy store unreachable")

func (outageAuthorizer) Authorize(context.Context, *models.Principal, string, ...string) error {
	return errAuthorizerUnavailable
}

func (outageAuthorizer) AuthorizeSilent(context.Context, *models.Principal, string, ...string) error {
	return errAuthorizerUnavailable
}

func (outageAuthorizer) FilterAuthorizedResources(context.Context, *models.Principal, string,
	...string,
) ([]string, error) {
	return nil, errAuthorizerUnavailable
}

// The cluster-wide check on the unattributable-task pass is a capability probe,
// not something the caller asked for: an ordinary single-collection cancel that
// merely coincides with such a task is answered 202 and must not leave an
// ERROR-level "authorization denied" behind for a request that succeeded. The
// grant is the opposite case — it is the one privileged act this pass gates, so
// it has to be attributable.
func TestCancelCapabilityProbeAuditsTheGrantButNotTheDenial(t *testing.T) {
	const (
		collection = "Movies"
		foreignID  = "Reviews:change_tokenization:body:ab3f"
	)

	tests := []struct {
		name       string
		authorizer func(logger logrus.FieldLogger) authorization.Authorizer
		wantStatus string
		// wantCancelled is the task the handler must have cancelled in DTM.
		wantCancelled string
		wantErrorLog  bool
		wantGrantLog  bool
		wantAuditity  string
	}{
		{
			name: "a caller holding only this collection is denied without an alert",
			authorizer: func(logger logrus.FieldLogger) authorization.Authorizer {
				return auditingAuthorizer{scopedAuthorizer: grantUpdateOn(collection), logger: logger}
			},
			wantStatus:   reindexCancelStatusNoOp,
			wantAuditity: "reindex_task_cancel_unattributable_denied",
		},
		{
			name: "a cluster-privileged caller cancels it, and the grant is on the record",
			authorizer: func(logger logrus.FieldLogger) authorization.Authorizer {
				// No classes named: the cluster-wide resource the probe asks
				// for, which is what UPDATE on every collection looks like.
				return auditingAuthorizer{scopedAuthorizer: grantUpdateOn(), logger: logger}
			},
			wantStatus:    "CANCELLED",
			wantCancelled: foreignID,
			wantGrantLog:  true,
			wantAuditity:  "reindex_task_cancel_unattributable_payload",
		},
		{
			name: "an authorizer that cannot answer leaves the task running, loudly",
			authorizer: func(logrus.FieldLogger) authorization.Authorizer {
				return outageAuthorizer{}
			},
			wantStatus:   reindexCancelStatusNoOp,
			wantErrorLog: true,
			wantAuditity: "reindex_task_cancel_unattributable_authorizer_unavailable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &raceTaskService{tasks: []*distributedtask.Task{
				unattributableTask(foreignID, distributedtask.TaskStatusStarted),
			}}
			var busy atomic.Bool
			h := submissionHandlers(t, svc, togglingProber{busy: &busy})

			logger, hook := logrustest.NewNullLogger()
			h.appState.Logger = logger
			h.appState.Authorizer = tc.authorizer(logger)
			h.appState.ReindexProvider.Store(db.NewReindexProvider(nil, nil, h.appState.Logger, fixtureNode,
				func() int { return 1 }, context.Background()))

			responder := h.cancelReindexTask(context.Background(), collection, "title", "filterable",
				&models.Principal{Username: "u1"})

			accepted, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
			require.Truef(t, ok, "cancel must be accepted, got %T", responder)
			require.Equal(t, tc.wantStatus, accepted.Payload.Status)

			if tc.wantCancelled == "" {
				require.Empty(t, svc.cancelled, "the task must be left running")
			} else {
				require.Len(t, svc.cancelled, 1)
				require.Equal(t, tc.wantCancelled, svc.cancelled[0].ID)
			}

			var errorLevel bool
			for _, entry := range hook.AllEntries() {
				errorLevel = errorLevel || entry.Level == logrus.ErrorLevel
			}
			require.Equalf(t, tc.wantErrorLog, errorLevel,
				"an error-level record is a page for a human; entries were %q", entryMessages(hook))

			grantLogged := strings.Contains(entryMessages(hook), auditGranted)
			require.Equalf(t, tc.wantGrantLog, grantLogged,
				"the audit stream has to carry the grant that let this cancel through, and nothing else; "+
					"entries were %q", entryMessages(hook))

			require.Contains(t, entryMessages(hook), tc.wantAuditity,
				"each outcome needs its own event name, or a SIEM rule counts two different facts as one")
		})
	}
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
