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

package errors

import (
	"bytes"
	"fmt"
	"syscall"
	"testing"

	pkgerrors "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/storagestate"
)

// Spelled out rather than composed from the constants: the id and the host
// are the contract the docs repo redirects from.
var mappingsFields = logrus.Fields{"docs_url": "https://docs.weaviate.io/e/core-mem001"}

func TestDocsLinkFields(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want logrus.Fields
	}{
		{
			name: "nil error",
			err:  nil,
			want: nil,
		},
		{
			name: "not enough mappings",
			err:  ErrNotEnoughMappings,
			want: mappingsFields,
		},
		{
			name: "not enough mappings wrapped as the shard init path wraps it",
			err:  pkgerrors.Wrap(ErrNotEnoughMappings, "memory pressure: cannot init shard"),
			want: mappingsFields,
		},
		{
			name: "not enough mappings wrapped via fmt.Errorf %w",
			err:  fmt.Errorf("add missing tenant shard t1 during update index: %w", ErrNotEnoughMappings),
			want: mappingsFields,
		},
		{
			name: "not enough memory is a different condition",
			err:  NewNotEnoughMemory("oom"),
			want: nil,
		},
		{
			name: "read-only shard",
			err:  storagestate.ErrStatusReadOnly,
			want: nil,
		},
		{
			name: "disk full",
			err:  syscall.ENOSPC,
			want: nil,
		},
		{
			name: "unrelated error",
			err:  fmt.Errorf("some other error"),
			want: nil,
		},
		{
			name: "message text alone does not qualify",
			err:  fmt.Errorf("not enough memory mappings"),
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, DocsLinkFields(tt.err))
		})
	}
}

func TestDocsLinkFieldsFor(t *testing.T) {
	tests := []struct {
		name string
		id   DocsID
		want logrus.Fields
	}{
		{
			name: "not enough mappings",
			id:   DocsIDNotEnoughMappings,
			want: mappingsFields,
		},
		{
			name: "any id resolves under the redirector",
			id:   DocsID("core-disk042"),
			want: logrus.Fields{"docs_url": "https://docs.weaviate.io/e/core-disk042"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, DocsLinkFieldsFor(tt.id))
		})
	}
}

// Log sites add DocsLinkFields unconditionally, which relies on logrus
// treating nil fields as no fields.
func TestDocsLinkFieldsNilIsNoOpInLogrus(t *testing.T) {
	var buf bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&buf)
	logger.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true, DisableColors: true})

	logger.WithFields(DocsLinkFields(fmt.Errorf("undocumented"))).Info("m")
	assert.Equal(t, "level=info msg=m\n", buf.String())

	buf.Reset()
	logger.WithFields(DocsLinkFields(ErrNotEnoughMappings)).Info("m")
	assert.Equal(t, "level=info msg=m docs_url=\"https://docs.weaviate.io/e/core-mem001\"\n", buf.String())
}

func TestMessageWithDocsLink(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil renders as fmt's <nil> placeholder", err: nil, want: "<nil>"},
		{name: "undocumented", err: fmt.Errorf("boom"), want: "boom"},
		{
			name: "documented",
			err:  ErrNotEnoughMappings,
			want: "not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
		{
			name: "documented and wrapped keeps the wrapping context",
			err:  fmt.Errorf("load shard t1: %w", ErrNotEnoughMappings),
			want: "load shard t1: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, MessageWithDocsLink(tt.err))
		})
	}
}

func TestAppendDocsLink(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		err  error
		want string
	}{
		{name: "undocumented keeps the caller's rendering", msg: "Articles: boom", err: fmt.Errorf("ns:Articles: boom"), want: "Articles: boom"},
		{
			name: "documented appends the page to the caller's rendering",
			msg:  "Articles: not enough memory mappings",
			err:  fmt.Errorf("ns:Articles: %w", ErrNotEnoughMappings),
			want: "Articles: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, AppendDocsLink(tt.msg, tt.err))
		})
	}
}

func TestErrGraphQLUserUnwrapsForDocsLinks(t *testing.T) {
	err := NewErrGraphQLUser(fmt.Errorf("explorer: %w", ErrNotEnoughMappings), "Get", "Demo")

	assert.ErrorIs(t, err, ErrNotEnoughMappings)
	assert.Equal(t, mappingsFields, DocsLinkFields(err))
}

func TestDocsLinkCarriesClusterID(t *testing.T) {
	t.Cleanup(func() { SetClusterIDSource(nil) })

	tests := []struct {
		name   string
		source func() string
		want   string
	}{
		{name: "no source", source: nil, want: "https://docs.weaviate.io/e/core-mem001"},
		{name: "id not committed yet", source: func() string { return "" }, want: "https://docs.weaviate.io/e/core-mem001"},
		{
			name:   "id known",
			source: func() string { return "0198c0de-dead-beef-8000-000000000001" },
			want:   "https://docs.weaviate.io/e/core-mem001?clusterid=0198c0de-dead-beef-8000-000000000001",
		},
		{
			name:   "id is query-escaped, never trusted",
			source: func() string { return "a b&c" },
			want:   "https://docs.weaviate.io/e/core-mem001?clusterid=a+b%26c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetClusterIDSource(tt.source)
			assert.Equal(t, tt.want, DocsLink(DocsIDNotEnoughMappings))
			assert.Equal(t, tt.want, DocsLinkFields(ErrNotEnoughMappings)["docs_url"])
			assert.Equal(t, "not enough memory mappings (see "+tt.want+")", MessageWithDocsLink(ErrNotEnoughMappings))
		})
	}
}
