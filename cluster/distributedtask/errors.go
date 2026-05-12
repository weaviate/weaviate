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

package distributedtask

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Sentinel errors describing stable, non-retryable FSM rejections from
// [Manager.RecordUnitCompletion], [Manager.UpdateUnitProgress] and similar
// apply paths.
//
// Classifiers (e.g. reindex_provider.isPermanentRecorderRejection) should
// use errors.Is against ErrPermanentRejection to decide whether to retry.
// The specific sentinels (ErrTaskNotRunning, ErrTaskDoesNotExist, ...) are
// kept so callers that want to log a precise reason can still test for
// them individually.
//
// ## Wire-format / mixed-version note
//
// In a cluster, the FSM error originates on the leader and reaches the
// caller via gRPC (cluster/rpc). gRPC transport collapses Go errors into
// (status.Code, message string) — wrapping with %w on the receiving side
// would lose the sentinel chain.
//
// To survive the round-trip we encode the sentinel identity into the gRPC
// error in two ways (both additive — no proto change required):
//
//  1. A stable [PermanentRejectionRPCCode] = codes.FailedPrecondition on
//     the [google.golang.org/grpc/status.Status]. This is the primary
//     discriminator.
//  2. A stable, machine-readable prefix marker on the error message, e.g.
//     "[dtm-perm/task-not-running] ...". The marker is the per-sentinel
//     fidelity carrier and survives any later %w wrapping because it lives
//     in the string. The marker is NOT user-facing; the human portion of
//     the message follows after the closing bracket.
//
// On the receiving side, [RehydratePermanentRejection] inspects an error
// from a gRPC apply call and re-attaches the appropriate sentinel via
// errors.Join so errors.Is keeps working end-to-end.
//
// Cross-version compatibility:
//   - Old leader → new follower: the leader returns the legacy
//     fmt.Errorf-formatted message without the marker and without
//     codes.FailedPrecondition. The follower's classifier falls back to
//     substring matching of the legacy phrases (with a Warn log so we
//     notice when the fallback fires in prod).
//   - New leader → old follower: the old follower still substring-matches
//     the legacy phrases, which we keep intact inside the new prefixed
//     messages. Both new and old phrasings coexist in the same string.
var (
	// ErrPermanentRejection is the umbrella sentinel matched by every
	// permanent FSM rejection. Classifiers should errors.Is against this.
	ErrPermanentRejection = errors.New("permanent FSM rejection")

	// ErrTaskNotRunning matches "task ... is no longer running".
	ErrTaskNotRunning = errors.New("task is no longer running")

	// ErrTaskDoesNotExist matches "task ... does not exist".
	ErrTaskDoesNotExist = errors.New("task does not exist")

	// ErrUnitAlreadyTerminal matches "unit ... is already terminal".
	ErrUnitAlreadyTerminal = errors.New("unit is already terminal")

	// ErrUnitWrongNode matches "unit ... belongs to node X, not Y".
	ErrUnitWrongNode = errors.New("unit belongs to a different node")
)

// PermanentRejectionRPCCode is the gRPC status code used to discriminate
// permanent FSM rejections from generic Internal errors on the wire. It
// is intentionally codes.FailedPrecondition: the FSM is internally
// consistent but the current request cannot be satisfied.
const PermanentRejectionRPCCode = codes.FailedPrecondition

// permanentMarker is the machine-readable prefix tag added to error
// messages so the receiving side can re-hydrate the specific sentinel
// even when the (status.Code, message) gRPC transport has erased the
// underlying error type.
//
// The format is "[dtm-perm/<id>] <human-readable message>".
type permanentMarker struct {
	sentinel error
	id       string
}

var permanentMarkers = []permanentMarker{
	{ErrTaskNotRunning, "task-not-running"},
	{ErrTaskDoesNotExist, "task-not-exist"},
	{ErrUnitAlreadyTerminal, "unit-terminal"},
	{ErrUnitWrongNode, "unit-wrong-node"},
}

// markerByID looks up a sentinel by its on-wire id.
func markerByID(id string) error {
	for _, m := range permanentMarkers {
		if m.id == id {
			return m.sentinel
		}
	}
	return nil
}

// idForSentinel returns the on-wire id for a sentinel, or "" if none.
func idForSentinel(sentinel error) string {
	for _, m := range permanentMarkers {
		if errors.Is(sentinel, m.sentinel) {
			return m.id
		}
	}
	return ""
}

// wrapPermanent produces an error that:
//   - matches errors.Is(_, sentinel) and errors.Is(_, ErrPermanentRejection)
//   - carries the on-wire marker prefix so the sentinel survives gRPC
//
// The human-readable msg is preserved after the marker so legacy
// substring matching against the old phrasing keeps working for old
// receivers (mixed-version cluster compat).
func wrapPermanent(sentinel error, msg string) error {
	id := idForSentinel(sentinel)
	prefixed := msg
	if id != "" {
		prefixed = fmt.Sprintf("[dtm-perm/%s] %s", id, msg)
	}
	// Chain so errors.Is hits both the specific sentinel and the umbrella.
	return fmt.Errorf("%s: %w", prefixed, errors.Join(sentinel, ErrPermanentRejection))
}

// ToRPCError converts an FSM error into a gRPC error that preserves the
// permanent-rejection classification across the wire. Generic (non-
// permanent) errors are returned as codes.Internal, matching the prior
// behavior in cluster/rpc.
//
// Callers in cluster/rpc/server.go should prefer this helper for any FSM
// path that may return permanent sentinels.
func ToRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrPermanentRejection) {
		return status.Error(PermanentRejectionRPCCode, err.Error())
	}
	return nil // signal to caller "not a DTM permanent rejection; handle normally"
}

// RehydratePermanentRejection inspects an error from a gRPC apply call
// and, if it carries the permanent-rejection signal, returns a new error
// that satisfies errors.Is for the appropriate sentinel(s).
//
// Detection order:
//  1. gRPC status code == PermanentRejectionRPCCode and the message
//     prefix matches "[dtm-perm/<id>] ..." → re-attach the specific
//     sentinel via errors.Join.
//  2. gRPC status code == PermanentRejectionRPCCode but the marker is
//     missing or unknown → re-attach only the umbrella sentinel
//     (forward-compat: a future sentinel id we don't recognise should
//     still be classified as permanent).
//
// If the error is not a recognised permanent-rejection signal, the
// input is returned unchanged.
func RehydratePermanentRejection(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	if st.Code() != PermanentRejectionRPCCode {
		return err
	}
	msg := st.Message()
	id := extractMarkerID(msg)
	if specific := markerByID(id); specific != nil {
		return errors.Join(err, specific, ErrPermanentRejection)
	}
	return errors.Join(err, ErrPermanentRejection)
}

// extractMarkerID returns the on-wire sentinel id from a message that
// starts with "[dtm-perm/<id>] ...". Returns "" if no marker is found.
func extractMarkerID(msg string) string {
	const prefix = "[dtm-perm/"
	if !strings.HasPrefix(msg, prefix) {
		return ""
	}
	rest := msg[len(prefix):]
	end := strings.Index(rest, "]")
	if end <= 0 {
		return ""
	}
	return rest[:end]
}
