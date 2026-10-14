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

package context

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
)

type contextKey string

func (c contextKey) String() string {
	return string(c)
}

const ctxPrincipalKey = contextKey("principal")

func GetPrincipalFromContext(ctx context.Context) *models.Principal {
	principal := ctx.Value(ctxPrincipalKey)
	if principal == nil {
		return nil
	}

	return principal.(*models.Principal)
}

func AddPrincipalToContext(ctx context.Context, principal *models.Principal) context.Context {
	return context.WithValue(ctx, ctxPrincipalKey, principal)
}

const ctxBatchNamespaceKey = contextKey("batch_namespace")

// BatchNamespace carries the namespace of a batch request from the handler
// back out to the REST middleware and the gRPC interceptor. The handler is
// the first place the principal, and so the namespace, is known. The
// middleware and the interceptor are the places the request size is known,
// and they label the size metric with the namespace.
//
// The handler writes the slot once, and the caller reads it once after the
// handler returns, on the same call chain. No lock is needed.
type BatchNamespace struct {
	Namespace string
}

// WithBatchNamespaceSlot returns an empty slot and a context carrying it.
// Callers install the slot before they invoke the handler chain, and read it
// after. The namespace stays empty when the handler never runs, and when the
// handler fails before it resolves the principal.
func WithBatchNamespaceSlot(ctx context.Context) (context.Context, *BatchNamespace) {
	slot := &BatchNamespace{}
	return context.WithValue(ctx, ctxBatchNamespaceKey, slot), slot
}

// SetBatchNamespace records the namespace in the slot installed by
// [WithBatchNamespaceSlot]. It is a no-op when no slot is installed, which is
// every code path with monitoring disabled.
func SetBatchNamespace(ctx context.Context, namespace string) {
	slot, ok := ctx.Value(ctxBatchNamespaceKey).(*BatchNamespace)
	if !ok {
		return
	}
	slot.Namespace = namespace
}
