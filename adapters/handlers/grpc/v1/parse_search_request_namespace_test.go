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

package v1

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/config"
)

// nsParserSchema is a parser-test fixture used only by the namespace-stitching
// tests. It deliberately stays separate from the package-level `scheme` so
// changes here don't churn the wide TestGRPCSearchRequest table.
//
// Both pairs of classes mirror what gets stored on a namespace-enabled cluster:
// the class identifier is qualified (`customer1:Zoo`) and the cross-ref
// property carries the short data type (`Animal`), since
// schema.ValidateClassName rejects ":" in cross-ref data types.
//
// The non-namespace pair (`Zoo` / `Animal` / `Habitat`) is used to assert the
// pass-through behavior on clusters where Namespaces.Enabled is false.
func nsParserClassGetter() classGetterWithAuthzFunc {
	classes := map[string]*models.Class{
		"customer1:Zoo": {
			Class: "customer1:Zoo",
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
				{Name: "hasAnimals", DataType: []string{"Animal"}},
			},
		},
		"customer1:Animal": {
			Class: "customer1:Animal",
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
				{Name: "hasHabitat", DataType: []string{"Habitat"}},
			},
		},
		"customer1:Habitat": {
			Class: "customer1:Habitat",
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
		},
		"Zoo": {
			Class: "Zoo",
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
				{Name: "hasAnimals", DataType: []string{"Animal"}},
			},
		},
		"Animal": {
			Class: "Animal",
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
		},
	}
	return func(name string) (*models.Class, error) {
		if c, ok := classes[name]; ok {
			return c, nil
		}
		return nil, fmt.Errorf("class %s not found", name)
	}
}

// TestExtractPropertiesRequest_NamespaceStitching covers the gRPC search
// parser's read-side namespace handling (WS9). The parser must qualify
// `linkedClassName` using the *parent class's* namespace before the
// authorized-class lookup, recursively for nested refs, and must reject any
// `prop.TargetCollection` that already carries a namespace prefix from a
// namespaced principal.
func TestExtractPropertiesRequest_NamespaceStitching(t *testing.T) {
	t.Run("single-target ref stitches parent namespace onto short DataType", func(t *testing.T) {
		parser := NewParser(false,
			nsParserClassGetter(),
			&models.Principal{Username: "u", Namespace: "customer1"}, true,
		)
		req := &pb.SearchRequest{
			Collection: "customer1:Zoo",
			Properties: &pb.PropertiesRequest{
				RefProperties: []*pb.RefPropertiesRequest{{
					ReferenceProperty: "hasAnimals",
					Properties:        &pb.PropertiesRequest{NonRefProperties: []string{"name"}},
				}},
			},
		}
		out, err := parser.Search(req, &config.Config{QueryDefaults: config.QueryDefaults{Limit: 10}})
		require.NoError(t, err)
		require.Len(t, out.Properties, 1)
		require.Len(t, out.Properties[0].Refs, 1)
		assert.Equal(t, "customer1:Animal", out.Properties[0].Refs[0].ClassName,
			"linkedClassName should be qualified using parent class's namespace")
	})

	t.Run("nested refs inherit namespace at each level", func(t *testing.T) {
		parser := NewParser(false,
			nsParserClassGetter(),
			&models.Principal{Username: "u", Namespace: "customer1"}, true,
		)
		req := &pb.SearchRequest{
			Collection: "customer1:Zoo",
			Properties: &pb.PropertiesRequest{
				RefProperties: []*pb.RefPropertiesRequest{{
					ReferenceProperty: "hasAnimals",
					Properties: &pb.PropertiesRequest{
						RefProperties: []*pb.RefPropertiesRequest{{
							ReferenceProperty: "hasHabitat",
							Properties:        &pb.PropertiesRequest{NonRefProperties: []string{"name"}},
						}},
					},
				}},
			},
		}
		out, err := parser.Search(req, &config.Config{QueryDefaults: config.QueryDefaults{Limit: 10}})
		require.NoError(t, err)
		require.Len(t, out.Properties, 1)
		require.Len(t, out.Properties[0].Refs, 1)
		assert.Equal(t, "customer1:Animal", out.Properties[0].Refs[0].ClassName)

		nested := out.Properties[0].Refs[0].RefProperties
		var habRef *string
		for i := range nested {
			if len(nested[i].Refs) > 0 {
				cls := nested[i].Refs[0].ClassName
				habRef = &cls
			}
		}
		require.NotNil(t, habRef, "expected a nested ref to Habitat")
		assert.Equal(t, "customer1:Habitat", *habRef,
			"nested linkedClassName should also qualify via parent's namespace")
	})

	t.Run("non-namespace cluster passes through unchanged", func(t *testing.T) {
		parser := NewParser(false,
			nsParserClassGetter(),
			&models.Principal{Username: "admin"}, false,
		)
		req := &pb.SearchRequest{
			Collection: "Zoo",
			Properties: &pb.PropertiesRequest{
				RefProperties: []*pb.RefPropertiesRequest{{
					ReferenceProperty: "hasAnimals",
					Properties:        &pb.PropertiesRequest{NonRefProperties: []string{"name"}},
				}},
			},
		}
		out, err := parser.Search(req, &config.Config{QueryDefaults: config.QueryDefaults{Limit: 10}})
		require.NoError(t, err)
		require.Len(t, out.Properties, 1)
		require.Len(t, out.Properties[0].Refs, 1)
		assert.Equal(t, "Animal", out.Properties[0].Refs[0].ClassName)
	})

	t.Run("namespaced caller cannot smuggle a namespace prefix via TargetCollection", func(t *testing.T) {
		// Force the multi-target branch by giving the property two DataType
		// entries. The schema-validation rule that rejects ":" in DataType
		// still holds at create time, but this test fabricates a multi-target
		// schema in-memory to exercise the parser's path.
		multiTarget := nsParserClassGetter()
		// Replace customer1:Zoo with a multi-target version.
		customGetter := func(name string) (*models.Class, error) {
			if name == "customer1:Zoo" {
				return &models.Class{
					Class: "customer1:Zoo",
					Properties: []*models.Property{
						{Name: "hasAnimals", DataType: []string{"Animal", "Habitat"}},
					},
				}, nil
			}
			return multiTarget(name)
		}
		parser := NewParser(false,
			customGetter,
			&models.Principal{Username: "u", Namespace: "customer1"}, true,
		)
		req := &pb.SearchRequest{
			Collection: "customer1:Zoo",
			Properties: &pb.PropertiesRequest{
				RefProperties: []*pb.RefPropertiesRequest{{
					ReferenceProperty: "hasAnimals",
					TargetCollection:  "customer2:Animal", // foreign-namespace target
					Properties:        &pb.PropertiesRequest{NonRefProperties: []string{"name"}},
				}},
			},
		}
		_, err := parser.Search(req, &config.Config{QueryDefaults: config.QueryDefaults{Limit: 10}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")
	})
}
