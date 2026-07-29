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
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

func TestDropVectorIndex_RejectVectorIndexTypeNone(t *testing.T) {
	none := vectorindex.VectorIndexTypeNone

	classWith := func(cfg map[string]models.VectorConfig) *models.Class {
		return &models.Class{Class: "C", VectorConfig: cfg}
	}

	tests := []struct {
		name       string
		prev       *models.Class
		next       *models.Class
		wantErr    bool
		wantTarget string
	}{
		{
			name:       "real -> none is rejected",
			prev:       classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: hnswT}}),
			next:       classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}}),
			wantErr:    true,
			wantTarget: "foo",
		},
		{
			name:       "absent -> none is rejected",
			prev:       classWith(map[string]models.VectorConfig{}),
			next:       classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}}),
			wantErr:    true,
			wantTarget: "foo",
		},
		{
			name:       "nil prev introducing none is rejected",
			prev:       nil,
			next:       classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}}),
			wantErr:    true,
			wantTarget: "foo",
		},
		{
			name: "existing none persists across an unrelated update",
			prev: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}}),
			next: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}}),
		},
		{
			name:       "none -> real type (revival) is rejected",
			prev:       classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}}),
			next:       classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: hnswT}}),
			wantErr:    true,
			wantTarget: "foo",
		},
		{
			name: "removing a dropped entry is not a revival (escalated + FSM-gated elsewhere)",
			prev: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: none}, "bar": {VectorIndexType: hnswT}}),
			next: classWith(map[string]models.VectorConfig{"bar": {VectorIndexType: hnswT}}),
		},
		{
			name: "all real types pass",
			prev: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: hnswT}}),
			next: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: hnswT}, "bar": {VectorIndexType: hnswT}}),
		},
		{
			name:       "one real one newly dropped reports the dropped one",
			prev:       classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: hnswT}, "bar": {VectorIndexType: hnswT}}),
			next:       classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: hnswT}, "bar": {VectorIndexType: none}}),
			wantErr:    true,
			wantTarget: "bar",
		},
		{
			name: "nil next passes",
			prev: classWith(map[string]models.VectorConfig{"foo": {VectorIndexType: hnswT}}),
			next: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rejectVectorIndexTypeNone(tt.prev, tt.next)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorContains(t, err, tt.wantTarget)
		})
	}
}

// TestDropVectorIndex_UpdateClassRejectsNoneIntroduction proves the C10 guard is
// wired into the client update path: a client cannot turn a live index into the
// "none" sentinel via UpdateClass (which would be a backdoor drop). The legit
// drop flow bypasses this path entirely via DeleteClassVectorIndex.
func TestDropVectorIndex_UpdateClassRejectsNoneIntroduction(t *testing.T) {
	ctx := context.Background()
	handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

	prev := &models.Class{
		Class: "DropSentinelClass",
		VectorConfig: map[string]models.VectorConfig{
			"foo": {
				VectorIndexType: hnswT,
				Vectorizer:      map[string]interface{}{"text2vec-contextionary": map[string]interface{}{}},
			},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	fakeSchemaManager.On("ReadOnlyClass", prev.Class).Return(prev)
	// Leader-consistent view agrees the index is live, so the rejection stands.
	fakeSchemaManager.On("QueryReadOnlyClasses", []string{prev.Class}).
		Return(map[string]versioned.Class{prev.Class: {Class: prev}}, nil)

	updated := &models.Class{
		Class: "DropSentinelClass",
		VectorConfig: map[string]models.VectorConfig{
			"foo": {VectorIndexType: modelsext.VectorIndexTypeNone},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}

	err := handler.UpdateClass(ctx, nil, updated.Class, updated)
	require.Error(t, err)
	require.ErrorContains(t, err, "internal sentinel for dropped indexes")
}

func TestDropVectorIndex_UpdateClassAllowsExistingNoneOnStaleNode(t *testing.T) {
	ctx := context.Background()
	handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

	className := "DropSentinelClass"
	// Stale local read: still shows foo as a live hnsw index.
	stale := &models.Class{
		Class: className,
		VectorConfig: map[string]models.VectorConfig{
			"foo": {
				VectorIndexType: hnswT,
				Vectorizer:      map[string]interface{}{"text2vec-contextionary": map[string]interface{}{}},
			},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	// Leader-consistent read: foo already dropped.
	dropped := &models.Class{
		Class: className,
		VectorConfig: map[string]models.VectorConfig{
			"foo": {
				VectorIndexType: modelsext.VectorIndexTypeNone,
				Vectorizer:      map[string]interface{}{"text2vec-contextionary": map[string]interface{}{}},
			},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	fakeSchemaManager.On("ReadOnlyClass", className).Return(stale)
	fakeSchemaManager.On("QueryReadOnlyClasses", []string{className}).
		Return(map[string]versioned.Class{className: {Class: dropped}}, nil)
	fakeSchemaManager.On("UpdateClass", mock.Anything, mock.Anything).Return(nil)

	updated := &models.Class{
		Class:       className,
		Description: "updated after drop",
		VectorConfig: map[string]models.VectorConfig{
			"foo": {
				VectorIndexType: modelsext.VectorIndexTypeNone,
				Vectorizer:      map[string]interface{}{"text2vec-contextionary": map[string]interface{}{}},
			},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}

	err := handler.UpdateClass(ctx, nil, className, updated)
	require.NoError(t, err)
}

// denyNthAuthorizer records every call on the inner fake and denies exactly
// the nth one — lets a test allow the metadata-scope check and deny the
// Collections escalation.
type denyNthAuthorizer struct {
	inner  *mocks.FakeAuthorizer
	n      int
	denyAt int
	deny   error
}

func (a *denyNthAuthorizer) Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	a.n++
	_ = a.inner.Authorize(ctx, principal, verb, resources...)
	if a.n == a.denyAt {
		return a.deny
	}
	return nil
}

func (a *denyNthAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return a.Authorize(ctx, principal, verb, resources...)
}

func (a *denyNthAuthorizer) FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	if err := a.Authorize(ctx, principal, verb, resources...); err != nil {
		return nil, err
	}
	return resources, nil
}

// TestUpdateClass_VectorEntryRemovalEscalatesToCollectionsScope pins the
// authz escalation on the generic update path: removing ANY VectorConfig
// entry demands the drop endpoint's Collections scope; an update that keeps
// every entry stays metadata-scoped.
func TestUpdateClass_VectorEntryRemovalEscalatesToCollectionsScope(t *testing.T) {
	principal := &models.Principal{}
	initial := func() *models.Class {
		return &models.Class{Class: "C", VectorConfig: map[string]models.VectorConfig{
			"keep": {VectorIndexType: hnswT},
			"gone": {VectorIndexType: modelsext.VectorIndexTypeNone},
		}}
	}

	t.Run("entry removal denied without Collections scope", func(t *testing.T) {
		inner := mocks.NewMockAuthorizer()
		denied := errors.New("collections scope denied")
		handler, fakeSchemaManager := newTestHandlerWithCustomAuthorizer(t, &fakeDB{},
			&denyNthAuthorizer{inner: inner, denyAt: 2, deny: denied})
		fakeSchemaManager.On("ReadOnlyClass", "C").Return(initial())

		updated := initial()
		delete(updated.VectorConfig, "gone")
		err := handler.UpdateClass(context.Background(), principal, "C", updated)
		require.ErrorIs(t, err, denied)

		calls := inner.Calls()
		require.Len(t, calls, 2)
		require.Equal(t, authorization.UPDATE, calls[0].Verb)
		require.Equal(t, authorization.CollectionsMetadata("C"), calls[0].Resources)
		require.Equal(t, authorization.UPDATE, calls[1].Verb)
		require.Equal(t, authorization.Collections("C"), calls[1].Resources,
			"removing a vector entry must demand the drop endpoint's scope")
	})

	t.Run("keeping every entry stays metadata-scoped", func(t *testing.T) {
		inner := mocks.NewMockAuthorizer()
		handler, fakeSchemaManager := newTestHandlerWithCustomAuthorizer(t, &fakeDB{},
			&denyNthAuthorizer{inner: inner, denyAt: 0})
		live := &models.Class{Class: "C", VectorConfig: map[string]models.VectorConfig{
			"keep":  {VectorIndexType: hnswT},
			"other": {VectorIndexType: hnswT},
		}}
		fakeSchemaManager.On("ReadOnlyClass", "C").Return(live).Maybe()
		fakeSchemaManager.On("QueryReadOnlyClasses", mock.Anything).Return(nil, nil).Maybe()
		fakeSchemaManager.On("UpdateClass", mock.Anything, mock.Anything).Return(nil).Maybe()

		// The downstream internal update may fail or panic on unrelated nil
		// fakes; the pin is only that NO second authorize fires first.
		func() {
			defer func() { _ = recover() }()
			_ = handler.UpdateClass(context.Background(), principal, "C", live)
		}()
		for i, call := range inner.Calls() {
			if i == 0 {
				continue
			}
			require.NotEqual(t, authorization.Collections("C"), call.Resources,
				"an update keeping every vector entry must not be escalated")
		}
		require.GreaterOrEqual(t, len(inner.Calls()), 1)
	})
}
