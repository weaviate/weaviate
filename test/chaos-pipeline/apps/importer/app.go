package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate-go-client/v3/weaviate"
	"github.com/semi-technologies/weaviate/entities/models"

	_ "net/http/pprof"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:5050", nil))
	}()

	if err := do(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func do(ctx context.Context) error {
	dims, err := getIntVar("DIMENSIONS")
	if err != nil {
		return err
	}

	shards, err := getIntVar("SHARDS")
	if err != nil {
		return err
	}

	size, err := getIntVar("SIZE")
	if err != nil {
		return err
	}

	batchSize, err := getIntVar("BATCH_SIZE")
	if err != nil {
		return err
	}

	origin, err := getStringVar("ORIGIN")
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

	httpClient := &http.Client{}

	count := 0
	beforeAll := time.Now()
	for count < size {
		batcher := newBatch()
		for i := 0; i < batchSize; i++ {
			batcher.addObject(fmt.Sprintf(`{"itemId":%d}`, count+1), randomVector(dims))
		}

		before := time.Now()
		if err := batcher.send(httpClient, origin); err != nil {
			return err
		}
		fmt.Printf("%f%% complete - last batch took %s - total %s\n",
			float32(count)/float32(size)*100,
			time.Since(before), time.Since(beforeAll))
		batcher = newBatch()
		count += batchSize
	}

	return nil
}

type batch struct {
	bytes.Buffer
	hasElements bool
}

func newBatch() *batch {
	b := &batch{}

	b.WriteString(`{"objects":[`)
	return b
}

func (b *batch) addObject(propsString string, vec []float32) {
	if b.hasElements {
		b.WriteString(",")
	}
	b.WriteString(fmt.Sprintf(`{"class":"DemoClass","properties":%s, "vector":[`, propsString))
	for i, dim := range vec {
		if i != 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%f", dim))
	}
	b.WriteString("]}")
	b.hasElements = true
}

func (b *batch) send(client *http.Client, origin string) error {
	b.WriteString("]}")

	body := b.Bytes()
	r := bytes.NewReader(body)

	req, err := http.NewRequest("POST", origin+"/v1/batch/objects", r)
	if err != nil {
		return err
	}

	req.Header.Add("content-type", "application/json")

	var res *http.Response
	attempt := 0
	res, err = client.Do(req)
	for err != nil {
		fmt.Printf("attempt %d failed with %s, try again in 1s...\n", attempt, err)
		time.Sleep(1 * time.Second)

		r.Seek(0, 0)

		if attempt == 100 {
			log.Printf("abort after 100 retries")
			return err
		}

		attempt++
		res, err = client.Do(req)
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		msg, _ := io.ReadAll(res.Body)

		return errors.Errorf("status %d: %s", res.StatusCode, string(msg))
	}

	io.ReadAll(res.Body)

	return nil
}

func randomVector(dim int) []float32 {
	out := make([]float32, dim)
	for i := range out {
		out[i] = rand.Float32()
	}
	return out
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

func getClass(shards int) *models.Class {
	return &models.Class{
		Class:           "DemoClass",
		Vectorizer:      "none",
		VectorIndexType: "hnsw",
		ShardingConfig: map[string]interface{}{
			"desiredCount": shards,
		},
		VectorIndexConfig: map[string]interface{}{
			"vectorCacheMaxObjects": 10000000000,
		},
		Properties: []*models.Property{
			{
				Name:     "itemId",
				DataType: []string{"int"},
			},
		},
	}
}
