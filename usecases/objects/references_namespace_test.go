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
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// zooAnimalNSSchema returns a Zoo/Animal schema. When qualify is true both
// the class identifier AND the cross-ref Property.DataType are stored under
// the "customer1:" namespace, matching what a namespace-enabled cluster
// persists after QualifyPropertyDataTypes runs at the schema handler
// (usecases/schema/class.go:AddClass). User-facing API validation rejects
// `:` in DataType, but QualifyPropertyDataTypes mutates the slice in place
// before it reaches RAFT, and storage keeps the qualified form. The
// reference handlers must therefore strip qualified DataType reads back to
// the short form for downstream resolveNS / beacon construction (see
// autodetectToClass and properties_validation.go).
func zooAnimalNSSchema(qualify bool) []*models.Class {
	zoo, animal := "Zoo", "Animal"
	hasAnimalsDT := []string{"Animal"}
	if qualify {
		zoo = "customer1:Zoo"
		animal = "customer1:Animal"
		hasAnimalsDT = []string{"customer1:Animal"}
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
				{Name: "hasAnimals", DataType: hasAnimalsDT},
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

// multiTargetNSSchema returns a Source class with a multi-target ref property
// hasOther pointing at [Alpha, Beta]. Used to exercise paths where autodetect
// cannot resolve the target class — autodetectToClass short-circuits on
// len(prop.DataType) > 1, so beacon.Class stays empty unless the caller
// supplied it explicitly.
func multiTargetNSSchema(qualify bool) []*models.Class {
	src, alpha, beta := "Source", "Alpha", "Beta"
	dt := []string{alpha, beta}
	if qualify {
		src = "customer1:Source"
		alpha = "customer1:Alpha"
		beta = "customer1:Beta"
		dt = []string{alpha, beta}
	}
	return []*models.Class{
		{
			Class:             src,
			VectorIndexConfig: hnsw.UserConfig{},
			Vectorizer:        config.VectorizerModuleNone,
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{Name: "hasOther", DataType: dt},
			},
		},
		{Class: alpha, VectorIndexConfig: hnsw.UserConfig{}, Vectorizer: config.VectorizerModuleNone},
		{Class: beta, VectorIndexConfig: hnsw.UserConfig{}, Vectorizer: config.VectorizerModuleNone},
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

	t.Run("NS: classless beacon on multi-target property is rejected with 400", func(t *testing.T) {
		// removeReference's NS path strips classes on both sides for the
		// structural compare, so an empty supplied class would wildcard
		// across every stored class for that TargetID — silently deleting
		// refs to unrelated classes on a multi-target property. The handler
		// gates this right after autodetect (which returns replace=false
		// on multi-target) with the same contract the write side uses in
		// validateReferenceMultiTenancy. The source-object lookup happens
		// upstream so its mock returns a minimal valid result.
		m, _, repo, _, _ := newNSManagers(t, multiTargetNSSchema(true), true)
		repo.On("Object", "customer1:Source", id, mock.Anything, mock.Anything, mock.Anything).
			Return(&search.Result{
				ClassName: "customer1:Source",
				Schema:    map[string]interface{}{"name": "src"},
			}, nil).Once()

		principal := &models.Principal{Username: "u", Namespace: "customer1"}
		input := &DeleteReferenceInput{
			Class: "Source", ID: id, Property: "hasOther",
			Reference: models.SingleRef{
				Beacon: strfmt.URI("weaviate://localhost/" + string(refID)),
			},
		}
		err := m.DeleteObjectReference(context.Background(), principal, input, nil, "")
		require.NotNil(t, err)
		assert.Equal(t, StatusBadRequest, err.Code)
		assert.Contains(t, err.Msg, "multi-target references require")
		repo.AssertExpectations(t)
	})

	t.Run("NS: classless beacon on single-target property still resolves via autodetect", func(t *testing.T) {
		// Negative control for the gate above: single-target classless
		// beacons must NOT trip the multi-target rejection. autodetect
		// fills beacon.Class from the schema's pre-qualified DataType
		// (stripped back to short), the gate sees beacon.Class != "" and
		// passes through. PutObject is expected because the source object
		// has no matching ref to delete (returns ok=false but the call
		// still doesn't error — assertion is that no 400 came back).
		m, _, repo, mp, _ := newNSManagers(t, zooAnimalNSSchema(true), true)
		repo.On("Object", "customer1:Zoo", id, mock.Anything, mock.Anything, mock.Anything).
			Return(&search.Result{
				ClassName: "customer1:Zoo",
				Schema:    map[string]interface{}{"name": "z"},
			}, nil).Once()
		mp.On("UsingRef2Vec", mock.Anything).Return(false)

		principal := &models.Principal{Username: "u", Namespace: "customer1"}
		input := &DeleteReferenceInput{
			Class: "Zoo", ID: id, Property: "hasAnimals",
			Reference: models.SingleRef{
				Beacon: strfmt.URI("weaviate://localhost/" + string(refID)),
			},
		}
		err := m.DeleteObjectReference(context.Background(), principal, input, nil, "")
		// No matching ref → handler returns nil (ok=false, errmsg="") without
		// hitting the multi-target gate. Asserting nil is the cleanest way
		// to pin "the gate did not fire".
		require.Nil(t, err,
			"single-target classless must pass the multi-target gate via autodetect")
		// Stored beacon is rewritten to the autodetected short class.
		assert.Equal(t,
			strfmt.URI("weaviate://localhost/Animal/"+string(refID)),
			input.Reference.Beacon,
			"autodetect should fill the short target class into the beacon")
		repo.AssertExpectations(t)
	})

	t.Run("non-NS: classless beacon on multi-target property is NOT rejected by the gate", func(t *testing.T) {
		// The gate is NS-only. Non-NS clusters use byte-exact compare in
		// removeReference, where a classless supplied beacon only matches a
		// classless stored beacon — no wildcard, no need for the gate.
		// Pin that the existing behavior survives: the call passes the gate
		// and reaches removeReference (which finds no match, returns nil).
		m, _, repo, mp, _ := newNSManagers(t, multiTargetNSSchema(false), false)
		repo.On("Object", "Source", id, mock.Anything, mock.Anything, mock.Anything).
			Return(&search.Result{
				ClassName: "Source",
				Schema:    map[string]interface{}{"name": "src"},
			}, nil).Once()
		mp.On("UsingRef2Vec", mock.Anything).Return(false)

		input := &DeleteReferenceInput{
			Class: "Source", ID: id, Property: "hasOther",
			Reference: models.SingleRef{
				Beacon: strfmt.URI("weaviate://localhost/" + string(refID)),
			},
		}
		err := m.DeleteObjectReference(context.Background(),
			&models.Principal{Username: "admin"}, input, nil, "")
		require.Nil(t, err,
			"non-NS classless on multi-target must pass the gate (it's NS-only)")
		repo.AssertExpectations(t)
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

	t.Run("MT-validation failure on one ref must not leak into authz or fail the batch", func(t *testing.T) {
		// Per-row error isolation: a ref that fails validateReferenceMultiTenancy
		// must be skipped from the READ-authz set entirely. Otherwise its
		// (qualified target, tenant) lands in the shard-paths slice and a
		// denial on it would 403 the WHOLE batch, even though
		// AddBatchReferences would have ignored the row (Err != nil).
		classes := []*models.Class{
			{
				Class:             "SourcePlain",
				VectorIndexConfig: hnsw.UserConfig{},
				Vectorizer:        config.VectorizerModuleNone,
				Properties: []*models.Property{
					{
						Name:         "name",
						DataType:     schema.DataTypeText.PropString(),
						Tokenization: models.PropertyTokenizationWhitespace,
					},
					{Name: "refTo", DataType: []string{"TargetPlain"}},
				},
			},
			{
				Class:              "TargetMT",
				VectorIndexConfig:  hnsw.UserConfig{},
				Vectorizer:         config.VectorizerModuleNone,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			},
			{
				Class:             "TargetPlain",
				VectorIndexConfig: hnsw.UserConfig{},
				Vectorizer:        config.VectorizerModuleNone,
			},
		}
		_, b, repo, _, authz := newNSManagers(t, classes, false)

		// Both refs must reach AddBatchReferences — the failing one marked
		// with Err, the valid one untouched.
		repo.On("AddBatchReferences", mock.MatchedBy(func(refs BatchReferences) bool {
			if len(refs) != 2 {
				return false
			}
			return refs[0].Err != nil &&
				strings.Contains(refs[0].Err.Error(), "multi-tenant") &&
				refs[1].Err == nil && refs[1].To != nil && refs[1].To.Class == "TargetPlain"
		})).Return(nil).Once()

		refs := []*models.BatchReference{
			// Bad: source is non-MT, target is MT — validateReferenceMultiTenancy rejects.
			{
				From: strfmt.URI("weaviate://localhost/SourcePlain/" + string(id) + "/refTo"),
				To:   strfmt.URI("weaviate://localhost/TargetMT/" + string(refID)),
			},
			// Good: both sides non-MT.
			{
				From: strfmt.URI("weaviate://localhost/SourcePlain/" + string(id) + "/refTo"),
				To:   strfmt.URI("weaviate://localhost/TargetPlain/" + string(refID)),
			},
		}
		_, err := b.AddReferences(context.Background(), &models.Principal{Username: "admin"}, refs, nil)
		require.NoError(t, err)

		// The failed ref's target class must NOT appear in any authz request.
		// Before the fix it would, because the (qualifiedTarget, tenant) entry
		// was added to uniqueClassShard regardless of refs[i].Err.
		for _, c := range authz.Calls() {
			for _, r := range c.Resources {
				assert.NotContains(t, r, "TargetMT",
					"authz must not see the errored ref's target class")
			}
		}
		repo.AssertExpectations(t)
	})

	t.Run("every ref fails MT validation: no empty-resource authz call, no 500", func(t *testing.T) {
		// Regression: when every ref hits a per-ref pre-authz error (here,
		// every MT existence check returns false), uniqueClassShard ends up
		// empty in the inner addReferences. Before the fix the function still
		// invoked b.authorizer.Authorize(READ) with an empty resources slice;
		// the real RBAC authorizer rejects that with "at least 1 resource is
		// required", which the REST handler maps to 500. The expected
		// behaviour is a 200 OK with per-ref errors preserved.
		classes := []*models.Class{
			{
				Class:              "SourceMT",
				VectorIndexConfig:  hnsw.UserConfig{},
				Vectorizer:         config.VectorizerModuleNone,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				Properties: []*models.Property{
					{Name: "refTo", DataType: []string{"TargetMT"}},
				},
			},
			{
				Class:              "TargetMT",
				VectorIndexConfig:  hnsw.UserConfig{},
				Vectorizer:         config.VectorizerModuleNone,
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			},
		}
		_, b, repo, _, authz := newNSManagers(t, classes, false)
		// Source object does not exist in tenant1 — validateTenantRefObject
		// fails the first existence check and aborts the rest of MT
		// validation for that ref. Mirrors the acceptance fixture
		// (TestAuthZBatchRefAuthZTenantFiltering) where the test sets up MT
		// classes but never creates the source object inside the tenant.
		repo.On("Exists", "SourceMT", id).Return(false, nil)
		// AddBatchReferences must still be invoked so per-ref Err propagates
		// back in the response payload — the 200-with-per-ref-errors contract
		// the REST layer relies on.
		repo.On("AddBatchReferences", mock.MatchedBy(func(refs BatchReferences) bool {
			if len(refs) != 2 {
				return false
			}
			for _, r := range refs {
				if r.Err == nil {
					return false
				}
			}
			return true
		})).Return(nil).Once()

		refs := []*models.BatchReference{
			{
				From:   strfmt.URI("weaviate://localhost/SourceMT/" + string(id) + "/refTo"),
				To:     strfmt.URI("weaviate://localhost/TargetMT/" + string(refID)),
				Tenant: "tenant1",
			},
			{
				From:   strfmt.URI("weaviate://localhost/SourceMT/" + string(id) + "/refTo"),
				To:     strfmt.URI("weaviate://localhost/TargetMT/" + string(refID)),
				Tenant: "tenant1",
			},
		}
		_, err := b.AddReferences(context.Background(), &models.Principal{Username: "admin"}, refs, nil)
		require.NoError(t, err)

		for _, c := range authz.Calls() {
			require.NotEmpty(t, c.Resources,
				"authz must never be called with an empty resources slice "+
					"(the real authorizer rejects it as a 500-level error)")
		}
		repo.AssertExpectations(t)
	})

	t.Run("mixed valid and malformed-target URIs: autodetectToClass survives nil To", func(t *testing.T) {
		// Regression: validateReference produces a BatchReference with To=nil
		// whenever crossref.Parse on the target URI fails. Before the fix
		// autodetectToClass evaluated `ref.To.Class != ""` before checking
		// ref.Err, panicking on the nil dereference. The bug surfaces only
		// when at least one valid ref keeps allClasses non-empty (so the
		// outer short-circuit doesn't fire); the panic would otherwise crash
		// the whole batch.
		_, b, repo, _, _ := newNSManagers(t, zooAnimalNSSchema(false), false)
		repo.On("AddBatchReferences", mock.MatchedBy(func(refs BatchReferences) bool {
			if len(refs) != 2 {
				return false
			}
			return refs[0].Err != nil && refs[1].Err == nil
		})).Return(nil).Once()

		refs := []*models.BatchReference{
			// Bad target URI: target is parsed as nil, ref.Err is set.
			{
				From: strfmt.URI("weaviate://localhost/Zoo/" + string(id) + "/hasAnimals"),
				To:   strfmt.URI("not-a-uri"),
			},
			// Good ref keeps the outer allClasses non-empty so we exercise
			// autodetectToClass with at least one nil-To entry alongside a
			// resolvable one.
			{
				From: strfmt.URI("weaviate://localhost/Zoo/" + string(id) + "/hasAnimals"),
				To:   strfmt.URI("weaviate://localhost/Animal/" + string(refID)),
			},
		}
		_, err := b.AddReferences(context.Background(), &models.Principal{Username: "admin"}, refs, nil)
		require.NoError(t, err)
		repo.AssertExpectations(t)
	})

	t.Run("every ref has a malformed URI: no empty-resource authz call, no 500", func(t *testing.T) {
		// Regression for the outer AddReferences. When every input ref has an
		// unparseable URI, the resolve/uniqueClass loops produce empty maps,
		// so allClasses ends up empty. Before the fix GetCachedClass was then
		// invoked with no names — its internal authz READ call hit the same
		// "at least 1 resource is required" path and returned a 500. The
		// expected response is the per-ref parse errors carried back to the
		// caller, not a server error.
		_, b, _, _, authz := newNSManagers(t, zooAnimalNSSchema(false), false)

		refs := []*models.BatchReference{
			{From: strfmt.URI("not-a-uri"), To: strfmt.URI("also-not-a-uri")},
			{From: strfmt.URI("weaviate://localhost"), To: strfmt.URI("weaviate://localhost")},
		}
		out, err := b.AddReferences(context.Background(), &models.Principal{Username: "admin"}, refs, nil)
		require.NoError(t, err)
		require.Len(t, out, 2)
		for _, r := range out {
			require.Error(t, r.Err)
		}
		for _, c := range authz.Calls() {
			require.NotEmpty(t, c.Resources,
				"authz must never be called with an empty resources slice")
		}
	})
}
