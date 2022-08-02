package main

import (
	"context"
	"log"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate-go-client/v3/weaviate"
	"github.com/semi-technologies/weaviate-go-client/v3/weaviate/graphql"
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

	log.Printf("starting async query load")
	stop := make(chan bool)
	go queryLoad(stop, client)

	count := 0
	beforeAll := time.Now()
	batcher := client.Batch().ObjectsBatcher()
	for count < size {
		for i := 0; i < batchSize; i++ {
			frequent := rand.Intn(50-20) + 20
			rare := rand.Intn(200-20) + 20
			batcher = batcher.WithObject(&models.Object{
				Class: "InvertedIndexOnly",
				Properties: map[string]interface{}{
					"text": GetWords(frequent, rare),
				},
			})
		}

		before := time.Now()
		if res, err := batcher.Do(ctx); err != nil {
			return err
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

		log.Printf("%f%% complete - last batch took %s - total %s\n",
			float32(count)/float32(size)*100,
			time.Since(before), time.Since(beforeAll))
		count += batchSize
	}

	stop <- true

	return nil
}

func queryLoad(stop chan bool, client *weaviate.Client) {
	t := time.Tick(2 * time.Millisecond)

	for {
		select {
		case <-stop:
			log.Printf("stopping async query load")
			return
		case <-t:
			sendFilteredReadQuery(client)
		}
	}
}

func sendFilteredReadQuery(client *weaviate.Client) {
	queryText := GetWords(3, 0)
	where := client.GraphQL().WhereArgBuilder().
		WithPath([]string{"text"}).
		WithOperator(graphql.Equal).
		WithValueText(queryText)

	res, err := client.GraphQL().Get().Objects().WithClassName("InvertedIndexOnly").
		WithLimit(10).
		WithWhere(where).
		WithFields([]graphql.Field{{Name: "_additional", Fields: []graphql.Field{
			{Name: "id"},
		}}}).
		Do(context.Background())
	if err != nil {
		log.Fatalf("ready query failed: %v", err)
	}

	if len(res.Errors) > 0 {
		log.Fatalf("ready query failed: %v", res.Errors[0].Message)
	}

	// objs := res.Data["Get"].(map[string]interface{})["InvertedIndexOnly"].([]interface{})
	// log.Printf("filter query returned %d results", len(objs))
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
		Class:      "InvertedIndexOnly",
		Vectorizer: "none",
		VectorIndexConfig: map[string]interface{}{
			"skip": true,
		},
		ShardingConfig: map[string]interface{}{
			"desiredCount": shards,
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{
			Stopwords: &models.StopwordConfig{
				Preset: "none",
			},
		},
		Properties: []*models.Property{
			{
				Name:     "text",
				DataType: []string{"text"},
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
