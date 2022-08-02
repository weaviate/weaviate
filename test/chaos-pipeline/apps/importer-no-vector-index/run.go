package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate-go-client/v3/weaviate"
	"github.com/semi-technologies/weaviate-go-client/v3/weaviate/batch"
	"github.com/semi-technologies/weaviate/entities/models"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	if err := do(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func do(ctx context.Context) error {
	batchSize, err := getIntVar("BATCH_SIZE")
	if err != nil {
		return err
	}

	size, err := getIntVar("SIZE")
	if err != nil {
		return err
	}

	origin, err := getStringVar("ORIGIN")
	if err != nil {
		return err
	}

	shards, err := getIntVar("SHARDS")
	if err != nil {
		return err
	}

	client, err := newClient(origin)
	if err != nil {
		return err
	}

	if err := client.Schema().AllDeleter().Do(ctx); err != nil {
		return err
	}

	if err := client.Schema().ClassCreator().WithClass(getClass(shards)).Do(ctx); err != nil {
		return err
	}

	count := 0
	beforeAll := time.Now()
	batcher := client.Batch().ObjectsBatcher()
	for count < size {
		before := time.Now()

		if err := buildAndSendBatchWithRetries(ctx, batcher, batchSize, 100, 5*time.Second); err != nil {
			return err
		}

		log.Printf("%f%% complete - last batch took %s - total %s\n",
			float32(count)/float32(size)*100,
			time.Since(before), time.Since(beforeAll))
		count += batchSize
	}

	return nil
}

func buildAndSendBatchWithRetries(ctx context.Context, batcher *batch.ObjectsBatcher, batchSize int,
	maxAttempts int, backoff time.Duration) error {
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			fmt.Printf("attempt %d, last error: %v\n", attempt, lastErr)
			time.Sleep(backoff)
		}

		for i := 0; i < batchSize; i++ {
			frequent := func() int { return rand.Intn(50-20) + 20 }
			rare := func() int { return rand.Intn(200-20) + 20 }
			batcher = batcher.WithObject(&models.Object{
				Class: "NoVector",
				Properties: map[string]interface{}{
					"text_1": GetWords(frequent(), rare()),
					"text_2": GetWords(frequent(), rare()),
					"text_3": GetWords(frequent(), rare()),
					"text_4": GetWords(frequent(), rare()),
					"text_5": GetWords(frequent(), rare()),
					"text_6": GetWords(frequent(), rare()),
					"text_7": GetWords(frequent(), rare()),
				},
			})
		}

		res, err := batcher.Do(ctx)
		if err != nil {
			lastErr = err
			continue
		} else {
			for _, c := range res {
				if c.Result != nil {
					if c.Result.Errors != nil && c.Result.Errors.Error != nil {
						return errors.Errorf("failed to create obj: %+v, with status: %v",
							c.Result.Errors.Error[0], c.Result.Status)
					}
				}
			}
		}

		return nil
	}

	return errors.Errorf("ultimately failed after %d attempts, last error was %v", maxAttempts, lastErr)
}

func getIntVar(envName string) (int, error) {
	v := os.Getenv(envName)
	if v == "" {
		return 0, errors.Errorf("missing required variable %s", envName)
	}

	asInt, err := strconv.Atoi(v)
	if err != nil {
		return 0, err
	}

	return asInt, nil
}

func getStringVar(envName string) (string, error) {
	v := os.Getenv(envName)
	if v == "" {
		return v, errors.Errorf("missing required variable %s", envName)
	}

	return v, nil
}

func getClass(shards int) *models.Class {
	return &models.Class{
		Class:      "NoVector",
		Vectorizer: "none",
		VectorIndexConfig: map[string]interface{}{
			"skip": true,
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": shards,
		},
		Properties: []*models.Property{
			{
				Name:     "text_1",
				DataType: []string{"text"},
			},
			{
				Name:     "text_2",
				DataType: []string{"text"},
			},
			{
				Name:     "text_3",
				DataType: []string{"text"},
			},
			{
				Name:     "text_4",
				DataType: []string{"text"},
			},
			{
				Name:     "text_5",
				DataType: []string{"text"},
			},
			{
				Name:     "text_6",
				DataType: []string{"text"},
			},
			{
				Name:     "text_7",
				DataType: []string{"text"},
			},
			{
				Name:     "blob",
				DataType: []string{"blob"},
			},
		},
	}
}

func newClient(origin string) (*weaviate.Client, error) {
	parsed, err := url.Parse(origin)
	if err != nil {
		return nil, err
	}

	cfg := weaviate.Config{
		Host:   parsed.Host,
		Scheme: parsed.Scheme,
	}
	return weaviate.New(cfg), nil
}
