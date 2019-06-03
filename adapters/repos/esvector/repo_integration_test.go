// +build integrationTest

package esvector

import (
	"context"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v5"
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
