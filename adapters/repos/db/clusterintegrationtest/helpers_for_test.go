//go:build integrationTest
// +build integrationTest

package clusterintegrationtest

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	enthnsw "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/stretchr/testify/require"
)

func setupDirectory(t *testing.T) string {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()
	return dirName
}

func dataAsBatch(data []*models.Object) objects.BatchObjects {
	batchObjs := make(objects.BatchObjects, len(data))
	for i := range data {
		batchObjs[i] = objects.BatchObject{
			OriginalIndex: i,
			Err:           nil,
			Object:        data[i],
			UUID:          data[i].ID,
			Vector:        data[i].Vector,
		}
	}

	return batchObjs
}

func dataAsBatchWithProps(data []*models.Object, props []string) objects.BatchObjects {
	batchObjs := make(objects.BatchObjects, len(data))
	for i := range data {
		batchObjs[i] = objects.BatchObject{
			OriginalIndex: i,
			Err:           nil,
			Object:        copyObjectWithProp(data[i], props),
			UUID:          data[i].ID,
			Vector:        data[i].Vector,
		}
	}

	return batchObjs
}

// copyObjectWithProp is not a 100% copy. It may still contain the same
// pointers in some properties, it does however guarantee that it does not
// alter the existing input - this guarantee is lost, if you modify the output
func copyObjectWithProp(in *models.Object, propsToCopy []string) *models.Object {
	out := &models.Object{}

	out.Additional = in.Additional
	out.Class = in.Class
	out.Vector = in.Vector
	out.CreationTimeUnix = in.CreationTimeUnix
	out.LastUpdateTimeUnix = in.LastUpdateTimeUnix
	out.ID = in.ID
	props := map[string]interface{}{}

	for _, propName := range propsToCopy {
		props[propName] = in.Properties.(map[string]interface{})[propName]
	}

	out.Properties = props
	return out
}

func multiShardState(nodeCount int) *sharding.State {
	config, err := sharding.ParseConfig(map[string]interface{}{
		"desiredCount": json.Number(fmt.Sprintf("%d", nodeCount)),
	}, 1)
	if err != nil {
		panic(err)
	}

	nodeList := make([]string, nodeCount)
	for i := range nodeList {
		nodeList[i] = fmt.Sprintf("node-%d", i)
	}

	s, err := sharding.InitState("multi-shard-test-index", config,
		fakeNodes{nodeList})
	if err != nil {
		panic(err)
	}

	return s
}

func class() *models.Class {
	cfg := enthnsw.NewDefaultUserConfig()
	cfg.EF = 500
	return &models.Class{
		Class:               distributedClass,
		VectorIndexConfig:   cfg,
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "description",
				DataType:     []string{string(schema.DataTypeText)},
				Tokenization: "word",
			},
			{
				Name:     "other_property",
				DataType: []string{string(schema.DataTypeText)},
			},
			{
				Name:     "date_property",
				DataType: []string{string(schema.DataTypeDate)},
			},
			{
				Name:     "date_array_property",
				DataType: []string{string(schema.DataTypeDateArray)},
			},
			{
				Name:     "int_property",
				DataType: []string{string(schema.DataTypeInt)},
			},
			{
				Name:     "phone_property",
				DataType: []string{string(schema.DataTypePhoneNumber)},
			},
		},
	}
}

func secondClassWithRef() *models.Class {
	cfg := enthnsw.NewDefaultUserConfig()
	cfg.EF = 500
	return &models.Class{
		Class:               "SecondDistributed",
		VectorIndexConfig:   cfg,
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:     "description",
				DataType: []string{string(schema.DataTypeText)},
			},
			{
				Name:     "toFirst",
				DataType: []string{distributedClass},
			},
		},
	}
}

func invertedConfig() *models.InvertedIndexConfig {
	return &models.InvertedIndexConfig{
		CleanupIntervalSeconds: 60,
	}
}

func exampleData(size int) []*models.Object {
	out := make([]*models.Object, size)

	for i := range out {
		vec := make([]float32, vectorDims)
		for i := range vec {
			vec[i] = rand.Float32()
		}

		timestamp := time.Unix(0, 0).Add(time.Duration(i) * time.Hour)
		phoneNumber := uint64(1000000 + rand.Intn(10000))

		out[i] = &models.Object{
			Class: distributedClass,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"description":         fmt.Sprintf("object-%d", i),
				"date_property":       timestamp,
				"date_array_property": []interface{}{timestamp},
				"int_property":        rand.Intn(1000),
				"phone_property": &models.PhoneNumber{
					CountryCode:            49,
					DefaultCountry:         "DE",
					Input:                  fmt.Sprintf("0171 %d", phoneNumber),
					Valid:                  true,
					InternationalFormatted: fmt.Sprintf("+49 171 %d", phoneNumber),
					National:               phoneNumber,
					NationalFormatted:      fmt.Sprintf("0171 %d", phoneNumber),
				},
			},
			Vector: vec,
		}
	}

	return out
}

func exampleDataWithRefs(size int, refCount int, targetObjs []*models.Object) []*models.Object {
	out := make([]*models.Object, size)

	for i := range out {
		vec := make([]float32, vectorDims)
		for i := range vec {
			vec[i] = rand.Float32()
		}

		refs := make(models.MultipleRef, refCount)
		for i := range refs {
			randomTarget := targetObjs[rand.Intn(len(targetObjs))]
			refs[i] = crossref.New("localhost", distributedClass, randomTarget.ID).SingleRef()
		}

		out[i] = &models.Object{
			Class: "SecondDistributed",
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"description": fmt.Sprintf("second-object-%d", i),
				"toFirst":     refs,
			},
			Vector: vec,
		}
	}

	return out
}

func bruteForceObjectsByQuery(objs []*models.Object,
	query []float32,
) []*models.Object {
	type distanceAndObj struct {
		distance float32
		obj      *models.Object
	}

	distProv := distancer.NewCosineDistanceProvider()
	distances := make([]distanceAndObj, len(objs))

	for i := range objs {
		dist, _, _ := distProv.SingleDist(normalize(query), normalize(objs[i].Vector))
		distances[i] = distanceAndObj{
			distance: dist,
			obj:      objs[i],
		}
	}

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	out := make([]*models.Object, len(objs))
	for i := range out {
		out[i] = distances[i].obj
	}

	return out
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

func manuallyResolveRef(t *testing.T, obj *models.Object,
	possibleTargets []*models.Object, localPropName,
	referencedPropName string,
) []map[string]interface{} {
	beacons := obj.Properties.(map[string]interface{})[localPropName].(models.MultipleRef)
	out := make([]map[string]interface{}, len(beacons))

	for i, ref := range beacons {
		parsed, err := crossref.Parse(ref.Beacon.String())
		require.Nil(t, err)
		target := findId(possibleTargets, parsed.TargetID)
		require.NotNil(t, target, "target not found")
		out[i] = map[string]interface{}{
			referencedPropName: target.Properties.(map[string]interface{})[referencedPropName],
		}
	}

	return out
}

func findId(list []*models.Object, id strfmt.UUID) *models.Object {
	for _, obj := range list {
		if obj.ID == id {
			return obj
		}
	}

	return nil
}

func refsAsBatch(in []*models.Object, propName string) objects.BatchReferences {
	out := objects.BatchReferences{}

	originalIndex := 0
	for _, obj := range in {
		beacons := obj.Properties.(map[string]interface{})[propName].(models.MultipleRef)
		current := make(objects.BatchReferences, len(beacons))
		for i, beacon := range beacons {
			to, err := crossref.Parse(beacon.Beacon.String())
			if err != nil {
				panic(err)
			}
			current[i] = objects.BatchReference{
				OriginalIndex: originalIndex,
				To:            to,
				From: crossref.NewSource(schema.ClassName(obj.Class),
					schema.PropertyName(propName), obj.ID),
			}
			originalIndex++
		}
		out = append(out, current...)
	}

	return out
}
