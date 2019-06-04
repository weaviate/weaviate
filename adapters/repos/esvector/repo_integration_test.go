// +build integrationTest

package esvector

import (
	"context"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEsVectorRepo(t *testing.T) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9201"},
	})
	require.Nil(t, err)
	waitForEsToBeReady(t, client)

	repo := NewRepo(client)

	t.Run("create the index", func(t *testing.T) {
		err := repo.PutIndex(context.Background(), "integrationtest")
		assert.Nil(t, err)
	})

	t.Run("sending the index creation request again", func(t *testing.T) {
		err := repo.PutIndex(context.Background(), "integrationtest")
		assert.Nil(t, err, "should not error, so index creation is idempotent")
	})

	t.Run("create the desired mappings", func(t *testing.T) {
		err := repo.SetMappings(context.Background(), "integrationtest")
		assert.Nil(t, err)
	})

	t.Run("update (upsert) the desired mappings", func(t *testing.T) {
		err := repo.SetMappings(context.Background(), "integrationtest")
		assert.Nil(t, err)
	})

	thingID := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")
	t.Run("adding a thing", func(t *testing.T) {
		thing := &models.Thing{
			ID:     thingID,
			Class:  "TheBestThingClass",
			Schema: map[string]interface{}{},
		}
		vector := []float32{1, 3, 5, 0.4}

		err := repo.PutThing(context.Background(), "integrationtest", thing, vector)

		assert.Nil(t, err)
	})

	actionID := strfmt.UUID("022ca5ba-7c0b-4a78-85bf-26346bbcfae7")
	t.Run("adding an action", func(t *testing.T) {
		action := &models.Action{
			ID:     actionID,
			Class:  "TheBestActionClass",
			Schema: map[string]interface{}{},
		}
		vector := []float32{3, 1, 0.3, 12}

		err := repo.PutAction(context.Background(), "integrationtest", action, vector)

		assert.Nil(t, err)
	})

	// sleep 2s to wait for index to become available
	time.Sleep(2 * time.Second)

	t.Run("searching by vector", func(t *testing.T) {
		// the search vector is designed to be very close to the action, but
		// somewhat far from the thing. So it should match the action closer
		searchVector := []float32{2.9, 1.1, 0.5, 8.01}

		res, err := repo.VectorSearch(context.Background(), "integrationtest", searchVector)

		require.Nil(t, err)
		require.Len(t, res, 2)
		assert.Equal(t, actionID, res[0].ID)
		assert.Equal(t, kind.Action, res[0].Kind)
		assert.Equal(t, "TheBestActionClass", res[0].ClassName)
		assert.Equal(t, "TheBestActionClass", res[0].ClassName)
		assert.Equal(t, thingID, res[1].ID)
		assert.Equal(t, kind.Thing, res[1].Kind)
		assert.Equal(t, "TheBestThingClass", res[1].ClassName)
	})
}

func waitForEsToBeReady(t *testing.T, client *elasticsearch.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	t.Log("waiting for ES to start up")
	defer cancel()

	var lastErr error

	for {
		if err := ctx.Err(); err != nil {
			t.Fatalf("es didn't start up in time: %v, last error: %v", err, lastErr)
		}

		_, err := client.Info()
		if err != nil {
			lastErr = err
		} else {
			return
		}

		time.Sleep(2 * time.Second)
	}
}
