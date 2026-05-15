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

package objects

import (
	"context"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// zooAnimalNSSchema returns a Zoo/Animal schema. When qualify is true the
// class identifiers are stored under the "customer1:" namespace, matching
// what a namespace-enabled cluster persists. The cross-ref DataType always
// stays short (`"Animal"`) because Property.DataType is validated via
// ValidateClassName which rejects the namespace separator — this is the
// invariant the reference handlers must compensate for by resolving
// autodetected targets through namespacing.Resolve.
func zooAnimalNSSchema(qualify bool) []*models.Class {
	zoo, animal := "Zoo", "Animal"
	if qualify {
		zoo = "customer1:Zoo"
		animal = "customer1:Animal"
	}
	return []*models.Class{
		{
			Class:             zoo,
			VectorIndexConfig: hnsw.UserConfig{},
			Vectorizer:        config.VectorizerModuleNone,
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{Name: "hasAnimals", DataType: []string{"Animal"}},
			},
		},
		{
			Class:             animal,
			VectorIndexConfig: hnsw.UserConfig{},
			Vectorizer:        config.VectorizerModuleNone,
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		},
	}
}

// newNSManagers returns a Manager + BatchManager wired against the same
// in-memory fakes, with namespaces toggled by nsEnabled. Both managers share
// the schema, repo and authorizer so a test can exercise single-ref and
// batch-ref paths through the same world.
func newNSManagers(t *testing.T, classes []*models.Class, nsEnabled bool) (*Manager, *BatchManager, *fakeVectorRepo, *fakeModulesProvider, *mocks.FakeAuthorizer) {
	t.Helper()
	sch := schema.Schema{Objects: &models.Schema{Classes: classes}}
	vectorRepo := &fakeVectorRepo{}
	cfg := &config.WeaviateConfig{
		Config: config.Config{
			AutoSchema: config.AutoSchema{Enabled: runtime.NewDynamicValue(false)},
			Namespaces: config.Namespaces{Enabled: nsEnabled},
		},
	}
	schemaManager := &fakeSchemaManager{GetSchemaResponse: sch}
	logger, _ := test.NewNullLogger()
	authorizer := mocks.NewMockAuthorizer()
	modulesProvider := getFakeModulesProvider()
	autoSchema := NewAutoSchemaManager(schemaManager, vectorRepo, cfg, logger, prometheus.NewPedanticRegistry())
	m := NewManager(schemaManager, cfg, logger, authorizer, vectorRepo, modulesProvider, &fakeMetrics{}, nil, autoSchema)
	b := NewBatchManager(vectorRepo, modulesProvider, schemaManager, cfg, logger, authorizer, nil, autoSchema)
	return m, b, vectorRepo, modulesProvider, authorizer
}

// Test_References_NamespaceResolution_Add covers AddObjectReference's two-view
// target handling: the qualified class drives in-memory authz and existence
// checks, while the *stored* beacon (passed to AddReference) carries the
// short class so the on-disk URI stays namespace-portable.
//
// Subtests:
//  1. namespaced principal, autodetected target: source qualifies to
//     customer1:Zoo for authz; existence-checks key on customer1:Animal; the
//     beacon written to the repo is short.
//  2. namespaced principal sending a foreign-namespace target via qualified
//     beacon: rejected with 422 at the prefix validator.
//  3. global admin on NS-enabled cluster submitting a qualified target
//     beacon: stored beacon is normalized to short (StripQualification).
//  4. global principal on a non-namespace cluster: short class names flow
//     through untouched (regression guard).
func Test_References_NamespaceResolution_Add(t *testing.T) {
	id := strfmt.UUID("d18c8e5e-0000-0000-0000-56b0cfe33ce7")
	refID := strfmt.UUID("d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")
	beacon := strfmt.URI("weaviate://localhost/" + string(refID))

	t.Run("namespaced principal: qualified ops, short on storage", func(t *testing.T) {
		m, _, repo, mp, authz := newNSManagers(t, zooAnimalNSSchema(true), true)
		repo.On("Exists", "customer1:Animal", refID).Return(true, nil).Once()
		repo.On("Exists", "customer1:Zoo", id).Return(true, nil).Once()
		repo.On("AddReference",
			mock.MatchedBy(func(s *crossref.RefSource) bool {
				return s != nil && string(s.Class) == "customer1:Zoo" && string(s.Property) == "hasAnimals" && s.TargetID == id
			}),
			mock.MatchedBy(func(target *crossref.Ref) bool {
				// Stored beacon must carry the SHORT target class.
				return target != nil && target.Class == "Animal" && target.TargetID == refID
			}),
		).Return(nil).Once()
		mp.On("UsingRef2Vec", mock.Anything).Return(false)

		principal := &models.Principal{Username: "u", Namespace: "customer1"}
		input := &AddReferenceInput{
			Class: "Zoo", ID: id, Property: "hasAnimals",
			Ref: models.SingleRef{Beacon: beacon},
		}
		err := m.AddObjectReference(context.Background(), principal, input, nil, "")
		require.Nil(t, err)

		require.NotEmpty(t, authz.Calls())
		assert.Contains(t, authz.Calls()[0].Resources[0], "customer1:Zoo")
		// The rewritten beacon on input.Ref reflects the storage shape too.
		assert.Equal(t, strfmt.URI("weaviate://localhost/Animal/"+string(refID)), input.Ref.Beacon)
		repo.AssertExpectations(t)
	})

	t.Run("namespaced principal sending qualified target beacon is rejected", func(t *testing.T) {
		m, _, repo, _, _ := newNSManagers(t, zooAnimalNSSchema(true), true)
		principal := &models.Principal{Username: "u", Namespace: "customer1"}
		input := &AddReferenceInput{
			Class: "Zoo", ID: id, Property: "hasAnimals",
			Ref: models.SingleRef{
				Beacon: strfmt.URI("weaviate://localhost/customer2:Animal/" + string(refID)),
			},
		}
		err := m.AddObjectReference(context.Background(), principal, input, nil, "")
		require.NotNil(t, err)
		assert.Equal(t, StatusUnprocessableEntity, err.Code)
		repo.AssertExpectations(t)
	})

	t.Run("global admin on NS cluster submitting qualified target writes short", func(t *testing.T) {
		// Admin addresses both source and target by their qualified storage
		// names (WS4 contract: admins use qualified). Our job is to make sure
		// the *stored beacon* still ends up short on disk for portability.
		m, _, repo, mp, _ := newNSManagers(t, zooAnimalNSSchema(true), true)
		repo.On("Exists", "customer1:Animal", refID).Return(true, nil).Once()
		repo.On("Exists", "customer1:Zoo", id).Return(true, nil).Once()
		repo.On("AddReference",
			mock.MatchedBy(func(s *crossref.RefSource) bool { return string(s.Class) == "customer1:Zoo" }),
			mock.MatchedBy(func(target *crossref.Ref) bool {
				return target.Class == "Animal" && target.TargetID == refID
			}),
		).Return(nil).Once()
		mp.On("UsingRef2Vec", mock.Anything).Return(false)

		admin := &models.Principal{Username: "admin"}
		input := &AddReferenceInput{
			Class: "customer1:Zoo", ID: id, Property: "hasAnimals",
			Ref: models.SingleRef{
				Beacon: strfmt.URI("weaviate://localhost/customer1:Animal/" + string(refID)),
			},
		}
		err := m.AddObjectReference(context.Background(), admin, input, nil, "")
		require.Nil(t, err)
		assert.Equal(t, strfmt.URI("weaviate://localhost/Animal/"+string(refID)), input.Ref.Beacon)
		repo.AssertExpectations(t)
	})

	t.Run("global principal on non-namespace cluster leaves class unchanged", func(t *testing.T) {
		m, _, repo, mp, authz := newNSManagers(t, zooAnimalNSSchema(false), false)
		repo.On("Exists", "Animal", refID).Return(true, nil).Once()
		repo.On("Exists", "Zoo", id).Return(true, nil).Once()
		repo.On("AddReference",
			mock.MatchedBy(func(s *crossref.RefSource) bool { return string(s.Class) == "Zoo" }),
			mock.MatchedBy(func(target *crossref.Ref) bool { return target.Class == "Animal" }),
		).Return(nil).Once()
		mp.On("UsingRef2Vec", mock.Anything).Return(false)

		input := &AddReferenceInput{
			Class: "Zoo", ID: id, Property: "hasAnimals",
			Ref: models.SingleRef{Beacon: beacon},
		}
		err := m.AddObjectReference(context.Background(), &models.Principal{Username: "admin"}, input, nil, "")
		require.Nil(t, err)
		require.NotEmpty(t, authz.Calls())
		assert.Contains(t, authz.Calls()[0].Resources[0], "/Zoo/")
		assert.NotContains(t, authz.Calls()[0].Resources[0], ":Zoo")
		repo.AssertExpectations(t)
	})
}

// Test_References_NamespaceResolution_Update covers UpdateObjectReferences for
// the namespaced happy path: source class and each entry in the multi-ref
// payload pass through resolveNS, so the schema lookup, authz, and existence
// checks all see the qualified names.
func Test_References_NamespaceResolution_Update(t *testing.T) {
	id := strfmt.UUID("d18c8e5e-0000-0000-0000-56b0cfe33ce7")
	refID := strfmt.UUID("d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")

	t.Run("namespaced principal qualifies source and explicit target", func(t *testing.T) {
		m, _, repo, _, authz := newNSManagers(t, zooAnimalNSSchema(true), true)
		// Source object lookup — the existing class fetch goes through
		// getObjectFromRepo which calls Object with the qualified class
		// because we pass an explicit (resolved) source class.
		repo.On("Object", "customer1:Zoo", id, mock.Anything, mock.Anything, mock.Anything).Return(nil, ErrNotFound{}).Once()

		principal := &models.Principal{Username: "u", Namespace: "customer1"}
		refs := models.MultipleRef{&models.SingleRef{
			Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(refID)),
			Class:  "Animal",
		}}
		input := &PutReferenceInput{
			Class: "Zoo", ID: id, Property: "hasAnimals", Refs: refs,
		}
		// UpdateObjectReferences fails at the source object lookup (we don't
		// have a real repo) — what we care about is that authz hit the
		// qualified class before the failure.
		err := m.UpdateObjectReferences(context.Background(), principal, input, nil, "")
		require.NotNil(t, err)
		require.NotEmpty(t, authz.Calls())
		assert.Contains(t, authz.Calls()[0].Resources[0], "customer1:Zoo")
	})
}

// Test_References_NamespaceResolution_Delete covers DeleteObjectReference's
// asymmetric handling: the source class is namespace-qualified for authz, but
// the target beacon is not — it's compared byte-for-byte against the stored
// short beacon. A namespaced user submitting a foreign-namespace target is
// still rejected with 422 via ValidateNamespacePrefix for consistency with
// add/update.
func Test_References_NamespaceResolution_Delete(t *testing.T) {
	id := strfmt.UUID("d18c8e5e-0000-0000-0000-56b0cfe33ce7")
	refID := strfmt.UUID("d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")

	t.Run("namespaced principal qualifies source class for authz", func(t *testing.T) {
		m, _, repo, _, authz := newNSManagers(t, zooAnimalNSSchema(true), true)
		// Source-object lookup goes through Object(qualifiedClass, ...). We
		// stub it to return ErrNotFound so DeleteObjectReference returns
		// early — the assertion is on the authz call that already ran with
		// the qualified class.
		repo.On("Object", "customer1:Zoo", id, mock.Anything, mock.Anything, mock.Anything).Return(nil, ErrNotFound{}).Once()

		principal := &models.Principal{Username: "u", Namespace: "customer1"}
		input := &DeleteReferenceInput{
			Class: "Zoo", ID: id, Property: "hasAnimals",
			Reference: models.SingleRef{
				Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(refID)),
			},
		}
		_ = m.DeleteObjectReference(context.Background(), principal, input, nil, "")
		require.NotEmpty(t, authz.Calls())
		assert.Contains(t, authz.Calls()[0].Resources[0], "customer1:Zoo")
		// Delete must not rewrite the input beacon — the user's short form
		// is exactly what removeReference compares against the stored value.
		assert.Equal(t, strfmt.URI("weaviate://localhost/Animal/"+string(refID)), input.Reference.Beacon)
	})

	t.Run("namespaced principal submitting cross-namespace target is rejected", func(t *testing.T) {
		m, _, _, _, _ := newNSManagers(t, zooAnimalNSSchema(true), true)
		principal := &models.Principal{Username: "u", Namespace: "customer1"}
		input := &DeleteReferenceInput{
			Class: "Zoo", ID: id, Property: "hasAnimals",
			Reference: models.SingleRef{
				Beacon: strfmt.URI("weaviate://localhost/customer2:Animal/" + string(refID)),
			},
		}
		err := m.DeleteObjectReference(context.Background(), principal, input, nil, "")
		require.NotNil(t, err)
		assert.Equal(t, StatusUnprocessableEntity, err.Code)
	})
}

// Test_References_NamespaceResolution_Batch proves BatchManager.AddReferences
// qualifies From.Class through resolveNS (for authz and schema lookup) while
// normalising the stored beacon target to the short form, so on-disk beacons
// stay namespace-portable. Per-ref Err isolation is preserved when one ref
// in a batch is a cross-namespace violation.
func Test_References_NamespaceResolution_Batch(t *testing.T) {
	id := strfmt.UUID("d18c8e5e-0000-0000-0000-56b0cfe33ce7")
	refID := strfmt.UUID("d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")

	t.Run("namespaced principal: qualified source, short stored target", func(t *testing.T) {
		_, b, repo, _, authz := newNSManagers(t, zooAnimalNSSchema(true), true)
		repo.On("AddBatchReferences", mock.MatchedBy(func(refs BatchReferences) bool {
			if len(refs) != 1 {
				return false
			}
			r := refs[0]
			// Source qualified (for shard routing), stored target short.
			return r.Err == nil &&
				string(r.From.Class) == "customer1:Zoo" &&
				r.To != nil && r.To.Class == "Animal"
		})).Return(nil).Once()

		principal := &models.Principal{Username: "u", Namespace: "customer1"}
		refs := []*models.BatchReference{{
			From: strfmt.URI("weaviate://localhost/Zoo/" + string(id) + "/hasAnimals"),
			To:   strfmt.URI("weaviate://localhost/Animal/" + string(refID)),
		}}
		_, err := b.AddReferences(context.Background(), principal, refs, nil)
		require.NoError(t, err)

		// Authorization must have been called against the qualified resource.
		seenQualified := false
		for _, c := range authz.Calls() {
			for _, r := range c.Resources {
				if strings.Contains(r, "customer1:Zoo") {
					seenQualified = true
				}
			}
		}
		assert.True(t, seenQualified, "expected authz to see qualified class")
		repo.AssertExpectations(t)
	})

	t.Run("foreign-namespace beacon class fails that ref only", func(t *testing.T) {
		_, b, repo, _, _ := newNSManagers(t, zooAnimalNSSchema(true), true)
		repo.On("AddBatchReferences", mock.MatchedBy(func(refs BatchReferences) bool {
			if len(refs) != 2 {
				return false
			}
			// First ref is bad (qualified target across namespaces); second is
			// fine. validateReferenceMultiTenancy / AddBatchReferences run on
			// the batch as a whole — we only care the bad ref carries an Err.
			return refs[0].Err != nil && refs[1].Err == nil
		})).Return(nil).Once()

		principal := &models.Principal{Username: "u", Namespace: "customer1"}
		refs := []*models.BatchReference{
			{
				From: strfmt.URI("weaviate://localhost/Zoo/" + string(id) + "/hasAnimals"),
				To:   strfmt.URI("weaviate://localhost/customer2:Animal/" + string(refID)),
			},
			{
				From: strfmt.URI("weaviate://localhost/Zoo/" + string(id) + "/hasAnimals"),
				To:   strfmt.URI("weaviate://localhost/Animal/" + string(refID)),
			},
		}
		_, err := b.AddReferences(context.Background(), principal, refs, nil)
		require.NoError(t, err)
		repo.AssertExpectations(t)
	})

	t.Run("global principal on non-namespace cluster passes through", func(t *testing.T) {
		_, b, repo, _, _ := newNSManagers(t, zooAnimalNSSchema(false), false)
		repo.On("AddBatchReferences", mock.MatchedBy(func(refs BatchReferences) bool {
			return len(refs) == 1 && string(refs[0].From.Class) == "Zoo" && refs[0].To.Class == "Animal"
		})).Return(nil).Once()

		refs := []*models.BatchReference{{
			From: strfmt.URI("weaviate://localhost/Zoo/" + string(id) + "/hasAnimals"),
			To:   strfmt.URI("weaviate://localhost/Animal/" + string(refID)),
		}}
		_, err := b.AddReferences(context.Background(), &models.Principal{Username: "admin"}, refs, nil)
		require.NoError(t, err)
		repo.AssertExpectations(t)
	})
}
