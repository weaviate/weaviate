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

package usagelimits

import (
	"context"
	"errors"
	"testing"

	"github.com/weaviate/weaviate/usecases/config/runtime"
)

type fakeObjectCounter struct {
	count int64
	err   error
}

func (f *fakeObjectCounter) LocalObjectCount(_ context.Context) (int64, error) {
	return f.count, f.err
}

type fakeCollectionCounter struct {
	count int64
	err   error
}

func (f *fakeCollectionCounter) LocalCollectionCount(_ context.Context) (int64, error) {
	return f.count, f.err
}

type fakeTenantCounter struct {
	counts map[string]int64
	err    error
}

func (f *fakeTenantCounter) LocalTenantCount(_ context.Context, class string) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.counts[class], nil
}

func TestManager_CheckObjects(t *testing.T) {
	tests := []struct {
		name    string
		cap     int
		current int64
		add     int64
		wantHit bool
		wantVal int64
	}{
		{name: "unlimited (-1)", cap: -1, current: 1000000, add: 1, wantHit: false},
		{name: "well under", cap: 10, current: 5, add: 1, wantHit: false},
		{name: "exactly at limit after add", cap: 10, current: 9, add: 1, wantHit: false},
		{name: "one over limit", cap: 10, current: 10, add: 1, wantHit: true, wantVal: 10},
		{name: "batch overflows by exactly one", cap: 10, current: 8, add: 3, wantHit: true, wantVal: 10},
		{name: "zero allowed rejects any add", cap: 0, current: 0, add: 1, wantHit: true, wantVal: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewManager(Config{
				MaxObjectsCount: runtime.NewDynamicValue(tt.cap),
				ErrorMessage:    runtime.NewDynamicValue(""),
			}, &fakeObjectCounter{count: tt.current}, nil, nil)

			err := m.CheckObjects(context.Background(), tt.add)
			limErr, ok := AsLimitExceeded(err)
			if tt.wantHit {
				if !ok {
					t.Fatalf("expected limit exceeded, got %v", err)
				}
				if limErr.Limit != LimitObjects {
					t.Errorf("Limit = %v, want %v", limErr.Limit, LimitObjects)
				}
				if limErr.Value != tt.wantVal {
					t.Errorf("Value = %d, want %d", limErr.Value, tt.wantVal)
				}
				if limErr.RenderedMessage == "" {
					t.Errorf("RenderedMessage should be populated from default template")
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestManager_CheckObjects_LoudOnMisconfig(t *testing.T) {
	// Limit configured but no counter wired → return error rather than
	// silently passing through.
	m := NewManager(Config{
		MaxObjectsCount: runtime.NewDynamicValue(10),
		ErrorMessage:    runtime.NewDynamicValue(""),
	}, nil, nil, nil)

	err := m.CheckObjects(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error when limit set but counter is nil")
	}
	if _, ok := AsLimitExceeded(err); ok {
		t.Fatal("misconfiguration error must NOT be a *LimitExceededError")
	}
}

func TestManager_CheckObjects_CounterError(t *testing.T) {
	m := NewManager(Config{
		MaxObjectsCount: runtime.NewDynamicValue(10),
		ErrorMessage:    runtime.NewDynamicValue(""),
	}, &fakeObjectCounter{err: errors.New("disk on fire")}, nil, nil)

	err := m.CheckObjects(context.Background(), 1)
	if err == nil {
		t.Fatal("expected counter error to propagate")
	}
	if _, ok := AsLimitExceeded(err); ok {
		t.Fatal("counter error must NOT be a *LimitExceededError")
	}
}

func TestManager_CheckCollections(t *testing.T) {
	m := NewManager(Config{
		MaxCollectionsCount: runtime.NewDynamicValue(3),
		ErrorMessage:        runtime.NewDynamicValue(""),
	}, nil, &fakeCollectionCounter{count: 3}, nil)

	if err := m.CheckCollections(context.Background(), 1); err == nil {
		t.Fatal("expected limit exceeded when adding 1 to 3 with cap 3")
	}

	m2 := NewManager(Config{
		MaxCollectionsCount: runtime.NewDynamicValue(3),
		ErrorMessage:        runtime.NewDynamicValue(""),
	}, nil, &fakeCollectionCounter{count: 2}, nil)
	if err := m2.CheckCollections(context.Background(), 1); err != nil {
		t.Fatalf("expected ok when adding 1 to 2 with cap 3, got %v", err)
	}
}

func TestManager_CheckTenants_PerCollection(t *testing.T) {
	tc := &fakeTenantCounter{counts: map[string]int64{"A": 2, "B": 0}}
	m := NewManager(Config{
		MaxTenantsPerCollection: runtime.NewDynamicValue(2),
		ErrorMessage:            runtime.NewDynamicValue(""),
	}, nil, nil, tc)

	if err := m.CheckTenants(context.Background(), "A", 1); err == nil {
		t.Errorf("expected reject for class A (already at cap)")
	}
	if err := m.CheckTenants(context.Background(), "B", 2); err != nil {
		t.Errorf("expected accept for class B (0 + 2 = cap), got %v", err)
	}
	if err := m.CheckTenants(context.Background(), "B", 3); err == nil {
		t.Errorf("expected reject for class B with batch of 3 (0 + 3 > 2)")
	}
}

func TestManager_CheckShards(t *testing.T) {
	m := NewManager(Config{
		MaxShardsPerCollection: runtime.NewDynamicValue(1),
		ErrorMessage:           runtime.NewDynamicValue(""),
	}, nil, nil, nil)

	if err := m.CheckShards(1); err != nil {
		t.Errorf("expected accept for shards=1 cap=1, got %v", err)
	}
	if err := m.CheckShards(2); err == nil {
		t.Errorf("expected reject for shards=2 cap=1")
	}
}

func TestManager_AllUnlimitedByDefault(t *testing.T) {
	// nil DynamicValues → unlimited (test that the Manager doesn't blow up
	// or reject when the Config is mostly zero-valued, e.g. during early
	// bootstrap).
	m := NewManager(Config{
		ErrorMessage: runtime.NewDynamicValue(""),
	}, nil, nil, nil)

	if err := m.CheckObjects(context.Background(), 1_000_000); err != nil {
		t.Errorf("expected unlimited objects, got %v", err)
	}
	if err := m.CheckCollections(context.Background(), 1_000); err != nil {
		t.Errorf("expected unlimited collections, got %v", err)
	}
	if err := m.CheckTenants(context.Background(), "any", 1_000); err != nil {
		t.Errorf("expected unlimited tenants, got %v", err)
	}
	if err := m.CheckShards(1_000); err != nil {
		t.Errorf("expected unlimited shards, got %v", err)
	}
}

func TestManager_NilManagerIsSafe(t *testing.T) {
	var m *Manager
	if err := m.CheckObjects(context.Background(), 1); err != nil {
		t.Errorf("nil manager CheckObjects should be safe, got %v", err)
	}
	if err := m.CheckShards(100); err != nil {
		t.Errorf("nil manager CheckShards should be safe, got %v", err)
	}
}

func TestManager_RenderedMessageUsesTemplate(t *testing.T) {
	m := NewManager(Config{
		MaxObjectsCount: runtime.NewDynamicValue(10),
		ErrorMessage:    runtime.NewDynamicValue("hit limit of {value} {limit}, upgrade at https://x"),
	}, &fakeObjectCounter{count: 10}, nil, nil)

	err := m.CheckObjects(context.Background(), 1)
	limErr, ok := AsLimitExceeded(err)
	if !ok {
		t.Fatalf("expected limit exceeded, got %v", err)
	}
	want := "hit limit of 10 objects, upgrade at https://x"
	if limErr.RenderedMessage != want {
		t.Errorf("RenderedMessage = %q, want %q", limErr.RenderedMessage, want)
	}
}
