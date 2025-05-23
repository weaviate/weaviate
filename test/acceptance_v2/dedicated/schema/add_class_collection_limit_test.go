package schema

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance_v2/dedicated"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestMaximumAllowedCollectionsCountLimit1(t *testing.T) {
	t.Parallel()

	compose := dedicated.SetupDedicated(t, func(t *testing.T) *docker.Compose {
		return docker.New().With1NodeCluster().WithWeaviateEnv("MAXIMUM_ALLOWED_COLLECTIONS_COUNT", "1")
	})
	client := helper.ClientFromURI(t, compose.GetWeaviate().URI())

	className := "TestCollection1"
	c1 := &models.Class{
		Class:      className,
		Vectorizer: "none",
	}

	// First class should succeed
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(c1)
	resp, err := client.Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	// Second class should fail
	className2 := "TestCollection2"
	c2 := &models.Class{
		Class:      className2,
		Vectorizer: "none",
	}

	params = schema.NewSchemaObjectsCreateParams().WithObjectClass(c2)
	resp, err = client.Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestFail(t, resp, err, func() {
		var parsed *schema.SchemaObjectsCreateUnprocessableEntity
		require.True(t, errors.As(err, &parsed), "error should be unprocessable entity")
		assert.Contains(t, parsed.Payload.Error[0].Message, "maximum number of collections")
	})

}

func TestMaximumAllowedCollectionsCountLimit100(t *testing.T) {
	t.Parallel()

	compose := dedicated.SetupDedicated(t, func(t *testing.T) *docker.Compose {
		return docker.New().With1NodeCluster().WithWeaviateEnv("MAXIMUM_ALLOWED_COLLECTIONS_COUNT", "100")
	})
	client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
	// Create classes up to the limit
	classNames := []string{}
	for i := 0; i < 100; i++ {
		className := fmt.Sprintf("TestCollection_%d", i)
		classNames = append(classNames, className)

		c := &models.Class{
			Class:      className,
			Vectorizer: "none",
		}

		params := schema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		resp, err := client.Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)
	}

	// Attempt to create one more class (should fail)
	c := &models.Class{
		Class:      "TestCollectionExtra",
		Vectorizer: "none",
	}

	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(c)
	resp, err := client.Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestFail(t, resp, err, func() {
		var parsed *schema.SchemaObjectsCreateUnprocessableEntity
		require.True(t, errors.As(err, &parsed), "error should be unprocessable entity")
		assert.Contains(t, parsed.Payload.Error[0].Message, "maximum number of collections")
	})

	// Delete one class and verify we can create a new one
	deleteParams := schema.NewSchemaObjectsDeleteParams().WithClassName(classNames[0])
	delResp, err := client.Schema.SchemaObjectsDelete(deleteParams, nil)
	helper.AssertRequestOk(t, delResp, err, nil)

	// Should now be able to create a new class
	params = schema.NewSchemaObjectsCreateParams().WithObjectClass(c)
	resp, err = client.Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func TestMaximumAllowedCollectionsCountUnlimited(t *testing.T) {
	t.Parallel()

	compose := dedicated.SetupDedicated(t, func(t *testing.T) *docker.Compose {
		return docker.New().With1NodeCluster()
	})
	client := helper.ClientFromURI(t, compose.GetWeaviate().URI())

	// Create multiple classes (more than default limit)
	classNames := []string{}
	for i := 0; i < 102; i++ {
		className := fmt.Sprintf("TestCollection_%d", i)
		classNames = append(classNames, className)

		c := &models.Class{
			Class:      className,
			Vectorizer: "none",
		}

		params := schema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		resp, err := client.Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)
	}

	// Verify all classes exist
	for _, className := range classNames {
		assert.Contains(t, GetObjectClassNamesWithClient(t, client), className)
	}
}

func GetObjectClassNamesWithClient(t *testing.T, client *client.Weaviate) []string {
	resp, err := client.Schema.SchemaDump(nil, nil)
	var names []string

	// Extract all names
	helper.AssertRequestOk(t, resp, err, func() {
		for _, class := range resp.Payload.Classes {
			names = append(names, class.Class)
		}
	})

	return names
}
