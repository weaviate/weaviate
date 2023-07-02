//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package stress_tests

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func TestConcurrentTenants(t *testing.T) {
	url := "http://localhost:8080/v1/"

	c := createHttpClient()
	clearExistingObjects(c, url)
	requestSchema := createSchemaRequest(url, class, true)

	performRequest(c, requestSchema)

	// Add schema and object
	parallelReqs := 50
	wg := sync.WaitGroup{}
	wgStartReqests := sync.WaitGroup{}
	wgStartReqests.Add(parallelReqs)
	wg.Add(parallelReqs)

	for i := 0; i < parallelReqs; i++ {
		go func(j int) {
			c := createHttpClient()

			requestAddTenant := createRequest(url+"schema/"+class+"/tenants", "POST", []models.Tenant{{Name: "tenant" + fmt.Sprint(j)}})

			wgStartReqests.Done()
			wgStartReqests.Wait()

			_, body, err := performRequest(c, requestAddTenant)
			var result []*models.Tenant
			json.Unmarshal(body, &result)

			if err != nil {
				panic(err)
			}

			wg.Done()
		}(i)
	}
	wg.Wait()

	requestGetTenants := createRequest(url+"schema/"+class+"/tenants", "GET", nil)

	_, body, _ := performRequest(c, requestGetTenants)
	var result []*models.Tenant
	json.Unmarshal(body, &result)
	assert.Equal(t, parallelReqs, len(result))
}
