package acceptance_with_go_client

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/test/helper/sample-schema/documents"

	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
)

var docTypes = []string{"private", "public"}

func TestWhereFilters(t *testing.T) {
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	ctx := context.Background()

	for _, class := range documents.ClassesContextionaryVectorizer(true) {
		c.Schema().ClassDeleter().WithClassName(class.Class).Do(ctx)
		require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))
	}
	defer c.Schema().ClassDeleter().WithClassName("Document").Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName("Passage").Do(ctx)

	var uuids []strfmt.UUID
	NDocs := 10
	for i := 0; i < NDocs; i++ {
		newUUID, err := uuid.NewUUID()
		require.Nil(t, err)
		uuids = append(uuids, strfmt.UUID(newUUID.String()))

		_, err = c.Data().Creator().WithClassName("Document").WithProperties(
			map[string]interface{}{
				"title":     fmt.Sprintf("Document title %v", i),
				"pageCount": i,
				"docType":   docTypes[i%2],
			},
		).WithID(newUUID.String()).Do(ctx)
		require.Nil(t, err)
	}

	for i := 0; i < NDocs; i++ {
		_, err := c.Data().Creator().WithClassName("Passage").WithProperties(
			map[string]interface{}{
				"content": fmt.Sprintf("Content of Passage %v", i),
				"ofDocument": []interface{}{
					map[string]interface{}{
						"beacon": crossref.NewLocalhost("Document", uuids[i]).String(),
					},
				},
			},
		).Do(ctx)
		require.Nil(t, err)
	}
	ids := graphql.Field{
		Name: "_additional", Fields: []graphql.Field{
			{Name: "id"},
		},
	}
	t.Run("Simple filter for documents AND", func(t *testing.T) {
		// both filters have the same result, which should apply to half of the documents => 50 results
		filter1 := filters.Where()
		filter1.WithOperator(filters.Equal)
		filter1.WithValueText(docTypes[0])
		filter1.WithPath([]string{"docType"})
		filter2 := filters.Where()
		filter2.WithOperator(filters.NotEqual)
		filter2.WithValueText(docTypes[1])
		filter2.WithPath([]string{"docType"})
		OuterWhere := filters.Where()
		OuterWhere.WithOperands([]*filters.WhereBuilder{filter1, filter2})
		OuterWhere.WithOperator(filters.And)
		result, err := c.GraphQL().Get().WithClassName("Document").WithWhere(filter1).WithFields(ids).Do(ctx)
		require.Nil(t, err)
		require.Len(t, result.Data["Get"].(map[string]interface{})["Document"], NDocs/2)
	})

	t.Run("Simple filter for documents OR", func(t *testing.T) {
		docType := graphql.Field{Name: "docType"}
		// both filters have half of the documents => 100 results
		filter1 := filters.Where()
		filter1.WithOperator(filters.Equal)
		filter1.WithValueText(docTypes[0])
		filter1.WithPath([]string{"docType"})
		filter2 := filters.Where()
		filter2.WithOperator(filters.NotEqual)
		filter2.WithValueText(docTypes[0])
		filter2.WithPath([]string{"docType"})
		OuterWhere := filters.Where()
		OuterWhere.WithOperands([]*filters.WhereBuilder{filter1, filter2})
		OuterWhere.WithOperator(filters.Or)
		result, err := c.GraphQL().Get().WithClassName("Document").WithWhere(filter1).WithFields(ids, docType).Do(ctx)
		require.Nil(t, err)
		require.Len(t, result.Data["Get"].(map[string]interface{})["Document"], NDocs)
	})

	t.Run("Simple filter for passage AND ", func(t *testing.T) {
		// both filters have the same result, which should apply to half of the documents => 50 results
		filter1 := filters.Where()
		filter1.WithOperator(filters.Equal)
		filter1.WithValueText(docTypes[0])
		filter1.WithPath([]string{"ofDocument", "Document", "docType"})
		filter2 := filters.Where()
		filter2.WithOperator(filters.NotEqual)
		filter2.WithValueText(docTypes[1])
		filter2.WithPath([]string{"ofDocument", "Document", "docType"})
		OuterWhere := filters.Where()
		OuterWhere.WithOperands([]*filters.WhereBuilder{filter1, filter2})
		OuterWhere.WithOperator(filters.And)
		result, err := c.GraphQL().Get().WithClassName("Passage").WithWhere(filter1).WithFields(ids).Do(ctx)
		require.Nil(t, err)
		require.Len(t, result.Data["Get"].(map[string]interface{})["Passage"], NDocs/2)
	})

	t.Run("Simple filter for passage OR ", func(t *testing.T) {
		// both filters have half of the documents => 100 results
		filter1 := filters.Where()
		filter1.WithOperator(filters.Equal)
		filter1.WithValueText(docTypes[0])
		filter1.WithPath([]string{"ofDocument", "Document", "docType"})
		filter2 := filters.Where()
		filter2.WithOperator(filters.NotEqual)
		filter2.WithValueText(docTypes[0])
		filter2.WithPath([]string{"ofDocument", "Document", "docType"})
		OuterWhere := filters.Where()
		OuterWhere.WithOperands([]*filters.WhereBuilder{filter1, filter2})
		OuterWhere.WithOperator(filters.And)
		result, err := c.GraphQL().Get().WithClassName("Passage").WithWhere(filter1).WithFields(ids).Do(ctx)
		require.Nil(t, err)
		require.Len(t, result.Data["Get"].(map[string]interface{})["Passage"], NDocs/2)
	})
}
