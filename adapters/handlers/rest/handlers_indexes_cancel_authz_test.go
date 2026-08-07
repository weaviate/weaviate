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

// scopedAuthorizer grants exactly the verb-and-resource pairs it was built with
// and denies every other pair, which is what an RBAC role holding UPDATE on one
// collection looks like to a handler. Keying on the verb as well as the
// resource is what makes a call site asking for the wrong privilege visible.
type scopedAuthorizer struct {
	granted map[string]bool
}

// grantKey is how one verb-on-one-resource privilege is spelled in granted.
func grantKey(verb, resource string) string {
	return verb + " on " + resource
}

// forbidden builds a denial the way both real authorizers do. Neither returns
// the Forbidden bare — rbac prefixes "rbac:" and adminlist prefixes
// "adminlist:" — so a caller that wants it has to unwrap.
func forbidden(principal *models.Principal, verb, resource string) error {
	return fmt.Errorf("rbac: %w", authzerrors.NewForbidden(principal, verb, resource))
}

func grantUpdateOn(classes ...string) *scopedAuthorizer {
	granted := map[string]bool{}
	for _, resource := range authorization.Collections(classes...) {
		granted[grantKey(authorization.UPDATE, resource)] = true
	}
	return &scopedAuthorizer{granted: granted}
}

func (a *scopedAuthorizer) Authorize(ctx context.Context, principal *models.Principal,
	verb string, resources ...string,
) error {
	for _, resource := range resources {
		if !a.granted[grantKey(verb, resource)] {
			return forbidden(principal, verb, resource)
		}
	}
	return nil
}

func (a *scopedAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal,
	verb string, resources ...string,
) error {
	return a.Authorize(ctx, principal, verb, resources...)
}

// FilterAuthorizedResources is here to satisfy the interface. No path under
// test reaches it, so it errors rather than carrying a second copy of the grant
// lookup that nothing would catch drifting.
func (a *scopedAuthorizer) FilterAuthorizedResources(ctx context.Context, principal *models.Principal,
	verb string, resources ...string,
) ([]string, error) {
	return nil, errors.New("scopedAuthorizer.FilterAuthorizedResources is not wired for these tests")
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

// splitAuthorizer lets the silent probe and the audible call disagree. The two
// take the RBAC store's lock separately, so a grant revoked between them
// answers the first and refuses the second, and an outage can start in the same
// window.
type splitAuthorizer struct {
	auditingAuthorizer
	audibleErr error
}

func (a splitAuthorizer) Authorize(ctx context.Context, principal *models.Principal,
	verb string, resources ...string,
) error {
	var forbidden authzerrors.Forbidden
	if errors.As(a.audibleErr, &forbidden) {
		a.logger.WithField("resource", resources).Error("authorization denied")
	}
	return a.audibleErr
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
		// wantGrantVerb is the verb the grant record has to carry. The record
		// exists to say which privilege was used, so the wrong verb on it is
		// the same as no record.
		wantGrantVerb  string
		wantAuditEvent string
		// wantEventLevel is the level of the entry carrying wantAuditEvent,
		// which is the handler's own verdict rather than the authorizer's.
		wantEventLevel logrus.Level
	}{
		{
			name: "a caller holding only this collection is denied without an alert",
			authorizer: func(logger logrus.FieldLogger) authorization.Authorizer {
				return auditingAuthorizer{scopedAuthorizer: grantUpdateOn(collection), logger: logger}
			},
			wantStatus:     reindexCancelStatusNoOp,
			wantAuditEvent: "reindex_task_cancel_unattributable_denied",
			wantEventLevel: logrus.InfoLevel,
		},
		{
			name: "a cluster-privileged caller cancels it, and the grant is on the record",
			authorizer: func(logger logrus.FieldLogger) authorization.Authorizer {
				// No classes named: the cluster-wide resource the probe asks
				// for, which is what UPDATE on every collection looks like.
				return auditingAuthorizer{scopedAuthorizer: grantUpdateOn(), logger: logger}
			},
			wantStatus:     "CANCELLED",
			wantCancelled:  foreignID,
			wantGrantLog:   true,
			wantGrantVerb:  authorization.UPDATE,
			wantAuditEvent: "reindex_task_cancel_unattributable_payload",
			wantEventLevel: logrus.InfoLevel,
		},
		{
			name: "an authorizer that cannot answer leaves the task running, loudly",
			authorizer: func(logrus.FieldLogger) authorization.Authorizer {
				return outageAuthorizer{}
			},
			wantStatus:     reindexCancelStatusNoOp,
			wantErrorLog:   true,
			wantAuditEvent: "reindex_task_cancel_unattributable_authorizer_unavailable",
			wantEventLevel: logrus.ErrorLevel,
		},
		{
			// The probe said yes and the record said no. Nothing is wrong with
			// the authorizer, so this is the denial arriving one statement
			// late, and it must not page anyone as an outage.
			name: "a grant withdrawn between the probe and the record leaves the task running",
			authorizer: func(logger logrus.FieldLogger) authorization.Authorizer {
				return splitAuthorizer{
					auditingAuthorizer: auditingAuthorizer{scopedAuthorizer: grantUpdateOn(), logger: logger},
					audibleErr: forbidden(&models.Principal{Username: "u1"},
						authorization.UPDATE, authorization.Collections()[0]),
				}
			},
			wantStatus: reindexCancelStatusNoOp,
			// The RBAC layer files its own denial for the audible call.
			wantErrorLog:   true,
			wantAuditEvent: "reindex_task_cancel_unattributable_grant_withdrawn",
			wantEventLevel: logrus.InfoLevel,
		},
		{
			// The same window, with the store going down in it rather than the
			// grant going away. Unknown, not absent, so it keeps the outage name.
			name: "an authorizer that fails only on the audible call leaves the task running, loudly",
			authorizer: func(logger logrus.FieldLogger) authorization.Authorizer {
				return splitAuthorizer{
					auditingAuthorizer: auditingAuthorizer{scopedAuthorizer: grantUpdateOn(), logger: logger},
					audibleErr:         errAuthorizerUnavailable,
				}
			},
			wantStatus:     reindexCancelStatusNoOp,
			wantErrorLog:   true,
			wantAuditEvent: "reindex_task_cancel_unattributable_authorizer_unavailable",
			wantEventLevel: logrus.ErrorLevel,
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

			grant := entryWithMessage(hook, auditGranted)
			require.Equalf(t, tc.wantGrantLog, grant != nil,
				"the audit stream has to carry the grant that let this cancel through, and nothing else; "+
					"entries were %q", entryMessages(hook))
			if tc.wantGrantVerb != "" {
				require.Equal(t, tc.wantGrantVerb, grant.Data["verb"],
					"the grant record names the privilege that was used; the wrong verb on it is "+
						"what a compliance query reads")
			}

			event := audited(hook, tc.wantAuditEvent)
			require.NotNilf(t, event,
				"a SIEM rule keys on audit_event, so this outcome has to file the one it declares; "+
					"entries were %q", entryMessages(hook))
			require.Equal(t, tc.wantEventLevel, event.Level,
				"the level is the handler's own verdict on this outcome, and a denial filed at "+
					"error level pages someone for a request that was answered correctly")
		})
	}
}

// entryMessages joins each entry's message with its audit_event field, so a
// failure can show what was logged without knowing which carries the text.
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
