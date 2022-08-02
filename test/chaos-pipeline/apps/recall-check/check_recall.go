package main

// +build ignore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate-go-client/v2/weaviate"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
	"github.com/semi-technologies/weaviate/entities/models"
)

var client *weaviate.Client

func init() {
	cfg := weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
	client = weaviate.New(cfg)
}

func main() {
	if err := do(); err != nil {
		log.Fatal(err)
	}
}

func do() error {
	file, err := os.Open("data/data.json")
	if err != nil {
		return err
	}

	data, err := read(file)
	if err != nil {
		return err
	}

	rand.Seed(1) // fixed seed for reproducibility

	queryCount := 200

	ids := randomRange(len(data), queryCount)

	meanRecall := float64(0)
	for _, id := range ids {
		queryVec := data[id].Vector

		limit := 10
		truths := sortByGroundTruth(data, queryVec)[:limit]

		res, err := retrieveFromWeaviate(queryVec, limit)
		if err != nil {
			return err
		}

		r := recall(res, truths, limit)
		meanRecall += r
		fmt.Printf(".")
	}

	meanRecall = meanRecall / float64(queryCount)
	fmt.Printf("\nMean Overall Recall: %f\n", meanRecall)

	return compareRecall(meanRecall)
}

func compareRecall(current float64) error {
	f, err := os.Open("data/recall_control")
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("no control file exists, assuming this run is the control " +
				"and writing the current results into a file.\n")
			return writeControl(current)
		}

		return err
	}

	defer f.Close()

	contents, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	control, err := strconv.ParseFloat(string(contents), 64)
	if err != nil {
		return err
	}

	if current >= control {
		fmt.Printf("Current (%f) is >= control (%f)\nPassed\n", current, control)
		return nil
	}

	diff := control - current
	if diff <= 0.01 {
		fmt.Printf("Current (%f) is less than control (%f) but within the margin of error\nPassed\n", current, control)
		return nil
	}

	return errors.Errorf("Current (%f) is < control (%f)\nPassed\n", current, control)
}

func writeControl(recall float64) error {
	f, err := os.Create("data/recall_control")
	if err != nil {
		return err
	}

	defer f.Close()

	asString := strconv.FormatFloat(recall, 'f', -1, 64)

	if _, err := f.Write([]byte(asString)); err != nil {
		return err
	}

	fmt.Printf("Control written.\n")

	return nil
}

func retrieveFromWeaviate(vec []float32, limit int) ([]obj, error) {
	ctx := context.Background()

	result, err := client.GraphQL().Get().Objects().
		WithClassName("SemanticUnit").
		WithFields("_additional { id }").
		WithNearVector(fmt.Sprintf("{vector: %v}", vec)).
		WithLimit(limit).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return parseGraphQLRes(result, "SemanticUnit")
}

func recall(candidates []obj, truths []obj, desiredLen int) float64 {
	desired := map[string]struct{}{}
	for _, relevant := range truths {
		desired[relevant.ID] = struct{}{}
	}

	var matches int
	for _, candidate := range candidates {
		_, ok := desired[candidate.ID]
		if ok {
			matches++
		}
	}

	return float64(matches) / float64(desiredLen)
}

func parseGraphQLRes(in *models.GraphQLResponse, className string) ([]obj, error) {
	list := in.Data["Get"].(map[string]interface{})[className].([]interface{})

	out := make([]obj, len(list))

	for i, elem := range list {
		id := elem.(map[string]interface{})["_additional"].(map[string]interface{})["id"]
		out[i] = obj{ID: id.(string)}
	}

	return out, nil
}

func read(file *os.File) ([]obj, error) {
	var out []obj
	err := json.NewDecoder(file).Decode(&out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

type obj struct {
	Properties map[string]interface{} `json:"properties"`
	Vector     []float32              `json:"vector"`
	ID         string                 `json:"id"`
}

func sortByGroundTruth(objs []obj,
	query []float32) []obj {
	type distanceAndObj struct {
		distance float32
		obj      obj
	}

	distances := make([]distanceAndObj, len(objs))

	for i := range objs {
		dist := 1 - asm.Dot(normalize(query), normalize(objs[i].Vector))
		distances[i] = distanceAndObj{
			distance: dist,
			obj:      objs[i],
		}
	}

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	out := make([]obj, len(objs))
	for i := range out {
		out[i] = distances[i].obj
	}

	return out
}

// assumes data is already in the correct order so that we can stop as soon as
// limit is reached
func filterByIntLessThanEqual(data []obj, prop string, value int, limit int) []obj {
	out := make([]obj, limit)
	i := 0

	for _, elem := range data {
		if i == limit {
			break
		}

		if elem.Properties == nil {
			continue
		}

		actual, ok := elem.Properties[prop]
		if !ok {
			continue
		}

		actualFloat := actual.(float64)
		if int(actualFloat) <= value {
			out[i] = elem
			i++
		}
	}

	return out[:i]
}

func normalize(v []float32) []float32 {
	var norm float32
	for i := range v {
		norm += v[i] * v[i]
	}

	norm = float32(math.Sqrt(float64(norm)))
	for i := range v {
		v[i] = v[i] / norm
	}

	return v
}

func randomRange(high, count int) []int {
	if count > high {
		panic(fmt.Sprintf("cannot pick %d out of %d numbers", count, high))
	}
	numbers := map[int]struct{}{}

	for len(numbers) < count {
		numbers[rand.Intn(high)] = struct{}{}
	}

	out := make([]int, len(numbers))
	i := 0
	for num := range numbers {
		out[i] = num
		i++
	}

	return out
}
