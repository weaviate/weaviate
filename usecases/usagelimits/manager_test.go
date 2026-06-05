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
	count        int64
	err          error
	gotNamespace string
}

func (f *fakeObjectCounter) LocalObjectCount(_ context.Context, namespace string) (int64, error) {
	f.gotNamespace = namespace
	return f.count, f.err
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
			}, &fakeObjectCounter{count: tt.current})

			err := m.CheckObjects(context.Background(), tt.add, "")
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
	}, nil)

	err := m.CheckObjects(context.Background(), 1, "")
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
	}, &fakeObjectCounter{err: errors.New("disk on fire")})

	err := m.CheckObjects(context.Background(), 1, "")
	if err == nil {
		t.Fatal("expected counter error to propagate")
	}
	if _, ok := AsLimitExceeded(err); ok {
		t.Fatal("counter error must NOT be a *LimitExceededError")
	}
}

func TestManager_UnlimitedByDefault(t *testing.T) {
	// nil DynamicValue → unlimited; nil counter is fine because we never
	// reach it.
	m := NewManager(Config{ErrorMessage: runtime.NewDynamicValue("")}, nil)
	if err := m.CheckObjects(context.Background(), 1_000_000, ""); err != nil {
		t.Errorf("expected unlimited objects, got %v", err)
	}
}

func TestManager_NilManagerIsSafe(t *testing.T) {
	var m *Manager
	if err := m.CheckObjects(context.Background(), 1, ""); err != nil {
		t.Errorf("nil manager CheckObjects should be safe, got %v", err)
	}
}

func TestManager_CheckObjects_ForwardsNamespace(t *testing.T) {
	// The cap is applied per-namespace: CheckObjects extracts the namespace
	// from the qualified class name and forwards it to the counter so the
	// right slice of indices is summed.
	tests := []struct {
		name          string
		className     string
		wantNamespace string
	}{
		{name: "qualified class name", className: "tenant-a:Movies", wantNamespace: "tenant-a"},
		{name: "plain class name", className: "Movies", wantNamespace: ""},
		{name: "empty class name", className: "", wantNamespace: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			counter := &fakeObjectCounter{count: 5}
			m := NewManager(Config{
				MaxObjectsCount: runtime.NewDynamicValue(10),
				ErrorMessage:    runtime.NewDynamicValue(""),
			}, counter)

			if err := m.CheckObjects(context.Background(), 1, tt.className); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if counter.gotNamespace != tt.wantNamespace {
				t.Errorf("counter saw namespace %q, want %q", counter.gotNamespace, tt.wantNamespace)
			}
		})
	}
}

func TestManager_RenderedMessageUsesTemplate(t *testing.T) {
	m := NewManager(Config{
		MaxObjectsCount: runtime.NewDynamicValue(10),
		ErrorMessage:    runtime.NewDynamicValue("hit limit of {value} {limit}, upgrade at https://x"),
	}, &fakeObjectCounter{count: 10})

	err := m.CheckObjects(context.Background(), 1, "")
	limErr, ok := AsLimitExceeded(err)
	if !ok {
		t.Fatalf("expected limit exceeded, got %v", err)
	}
	want := "hit limit of 10 objects, upgrade at https://x"
	if limErr.RenderedMessage != want {
		t.Errorf("RenderedMessage = %q, want %q", limErr.RenderedMessage, want)
	}
}
