//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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

func TestConcurrentTenantsAddAndRemove(t *testing.T) {
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
			requestDeleteTenant := createRequest(url+"schema/"+class+"/tenants", "DELETE", []string{"tenant" + fmt.Sprint(j)})

			wgStartReqests.Done()
			wgStartReqests.Wait()
			performRequest(c, requestAddTenant)
			performRequest(c, requestDeleteTenant)

			wg.Done()
		}(i)
	}
	wg.Wait()

	requestGetTenants := createRequest(url+"schema/"+class+"/tenants", "GET", nil)

	_, body, _ := performRequest(c, requestGetTenants)
	var result []*models.Tenant
	json.Unmarshal(body, &result)
	assert.Equal(t, 0, len(result))
}

func TestConcurrentTenantBatchesWithTenantAdd(t *testing.T) {
	url := "http://localhost:8080/v1/"

	c := createHttpClient()
	clearExistingObjects(c, url)

	tenants := make([]models.Tenant, 10)
	for i := 0; i < len(tenants); i++ {
		tenants[i] = models.Tenant{Name: "tenant" + fmt.Sprint(i)}
	}

	nrClasses := 10
	for i := 0; i < nrClasses; i++ {
		performRequest(c, createSchemaRequest(url, class+fmt.Sprint(i), true))
		performRequest(c, createRequest(url+"schema/"+class+fmt.Sprint(i)+"/tenants", "POST", tenants))
	}

	// Add schema and object
	parallelReqs := 20
	wg := sync.WaitGroup{}
	wgStartReqests := sync.WaitGroup{}
	wgStartReqests.Add(parallelReqs)
	wg.Add(parallelReqs)

	batchSize := 100

	for i := 0; i < parallelReqs; i++ {
		go func(j int) {
			c := createHttpClient()

			requestBatch := createRequest(url+"batch/objects", "POST", batch{createBatch(class+fmt.Sprint(j%nrClasses), batchSize, tenants)})

			wgStartReqests.Done()
			wgStartReqests.Wait()
			performRequest(c, requestBatch)
			wg.Done()
		}(i)
	}
	wg.Wait()

	// check for one class that we have the expected nr of objects per tenant
	requestRead := createRequest(url+fmt.Sprintf("objects?limit=%v&class="+class+"%s&tenant=%s", 1000, "4", fmt.Sprint(tenants[0].Name)), "GET", nil)
	_, body, _ := performRequest(c, requestRead)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	assert.Equal(t, parallelReqs/nrClasses*batchSize/len(tenants), int(result["totalResults"].(float64))) // batches per class, * objects per batch/nr_tenants
	clearExistingObjects(c, url)
}
