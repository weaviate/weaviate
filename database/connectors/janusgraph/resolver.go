package janusgraph

import (
	"fmt"
	graphql_local_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
)

func (j *Janusgraph) LocalGetClass(params *graphql_local_get.LocalGetClassParams) (func() interface{}, error) {
	first := 100
	offset := 0

	if params.Pagination != nil {
		first = params.Pagination.First
		offset = params.Pagination.After
	}

	type resolveResult struct {
		results []interface{}
		err     error
	}

	ch := make(chan resolveResult, 1)

	go func() {
		defer close(ch)
		results := []interface{}{}

		err := j.listClass(params.Kind, first, offset, "", nil, func(uuid strfmt.UUID) {
			var (
				atClass            string
				atContext          string
				foundUUID          strfmt.UUID
				creationTimeUnix   int64
				lastUpdateTimeUnix int64
				properties         models.Schema
				key                *models.SingleRef
			)

			err := j.getClass(params.Kind, uuid, &atClass, &atContext, &foundUUID, &creationTimeUnix, &lastUpdateTimeUnix, &properties, &key)

			if err == nil {
				result := map[string]interface{}{
					"uuid": interface{}(foundUUID),
				}
				propertiesMap := properties.(map[string]interface{})
				for propName, propValue := range propertiesMap {
					result[propName] = propValue
				}
				results = append(results, interface{}(result))
			} else {
				// Silently ignorre the potentially removed things.
			}
		})

		if err != nil {
			ch <- resolveResult{err: fmt.Errorf("Janusgraph.LocalGetClass: %#v", err)}
		} else {
			ch <- resolveResult{results: results}
		}
	}()

	return func() interface{} {
		result := <-ch
		if result.err != nil {
			panic(result.err)
		}
		return result.results
	}, nil
}
