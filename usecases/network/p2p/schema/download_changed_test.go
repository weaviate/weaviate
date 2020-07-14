//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/stretchr/testify/assert"
)

func TestDownloadPeersWithChanges(t *testing.T) {

	var (
		initialPeers    peers.Peers
		updatedPeers    peers.Peers
		p1server        *httptest.Server
		p2server        *httptest.Server
		p3server        *httptest.Server
		p1updatedSchema schema.Schema
		p2updatedSchema schema.Schema
		p3updatedSchema schema.Schema
	)

	arrange := func(p1matchers, p2matchers, p3matchers []requestMatcher) {
		p1updatedSchema = schemaWithThingClasses("shouldnot", "matter")
		p2updatedSchema = schemaWithThingClasses("p2newClass1", "p2NewClass2")
		p3updatedSchema = schemaWithThingClasses("p3newClass1", "p3NewClass2")
		p1server = schemaEndpoint(t, p1updatedSchema, p1matchers...)
		p2server = schemaEndpoint(t, p2updatedSchema, p2matchers...)
		p3server = schemaEndpoint(t, p3updatedSchema, p3matchers...)
		initialPeers = peers.Peers{
			{
				Name:       "peer1",
				ID:         "peer1",
				URI:        strfmt.URI(p1server.URL),
				LastChange: peers.NoChange,
				Schema:     schemaWithThingClasses("p1oldClass1", "p1oldClass2"),
			},
			{
				Name:       "peer2",
				ID:         "peer2",
				URI:        strfmt.URI(p2server.URL),
				LastChange: peers.SchemaChange,
				Schema:     schemaWithThingClasses("p2oldClass1", "p2oldClass2"),
			},
			{
				Name:       "peer3",
				ID:         "peer3",
				URI:        strfmt.URI(p3server.URL),
				LastChange: peers.NewlyAdded,
			},
		}
	}

	act := func() {
		updatedPeers = DownloadChanged(initialPeers)
	}

	cleanUp := func() {
		p1server.Close()
		p2server.Close()
		p2server.Close()
	}

	t.Run("result should contain the updated schemas", func(t *testing.T) {
		arrange(nil, nil, nil)
		act()
		cleanUp()

		assert.EqualValues(t, updatedPeers[0].Schema, initialPeers[0].Schema,
			"p1 should still have the same schema")

		assert.Equal(t, p2updatedSchema, updatedPeers[1].Schema,
			"p2 should have the updated schema")

		assert.Equal(t, p3updatedSchema, updatedPeers[2].Schema,
			"p3 should have the updated schema")
	})

	t.Run("peer 1 should be never be called", func(t *testing.T) {
		called := false
		matcher := func(t *testing.T, r *http.Request) {
			called = true
		}
		arrange([]requestMatcher{matcher}, nil, nil)
		act()

		if called != false {
			t.Error("p1 schema handler should never be called, but was called!")
		}

		cleanUp()
	})

	t.Run("peer 2 should be called", func(t *testing.T) {
		called := false
		matcher := func(t *testing.T, r *http.Request) {
			called = true
		}
		arrange(nil, []requestMatcher{matcher}, nil)
		act()

		if called == false {
			t.Error("p2 should be called but wasn't")
		}

		cleanUp()
	})

	t.Run("peer 3 should be called", func(t *testing.T) {
		called := false
		matcher := func(t *testing.T, r *http.Request) {
			called = true
		}
		arrange(nil, nil, []requestMatcher{matcher})
		act()

		if called == false {
			t.Error("p3 should be called but wasn't")
		}

		cleanUp()
	})
}

func schemaWithThingClasses(names ...string) schema.Schema {
	classes := make([]*models.Class, len(names), len(names))
	for _, class := range names {
		classes = append(classes,
			&models.Class{
				Class: class,
			},
		)
	}

	return schema.Schema{
		Actions: &models.Schema{
			Classes: classes,
		},
	}
}

func schemaEndpoint(t *testing.T, schemaToReturn schema.Schema, matchers ...requestMatcher) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, matcher := range matchers {
			matcher(t, r)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(schemaToReturn)
	}))
	return ts
}
