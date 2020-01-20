//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package esvector

// // this is not a test suite in itself, but it makes sure that other test suites
// // which depend no a specific caching state are run sequentially in a specific
// // order, so that they don't interefere with each other

// func Test_OrchestrateCaching(t *testing.T) {
// 	// requires caching to be stopped initially, starts caching at some point in
// 	// the test suite, stops caching in the end to clean up
// 	testEsVectorCache(t)

// 	client, err := elasticsearch.NewClient(elasticsearch.Config{
// 		Addresses: []string{"http://localhost:9201"},
// 	})
// 	require.Nil(t, err)
// 	schemaGetter := &fakeSchemaGetter{schema: parkingGaragesSchema()}
// 	logger := logrus.New()
// 	repo := NewRepo(client, logger, schemaGetter, 2, 100, 1, "0-1")
// 	waitForEsToBeReady(t, repo)
// 	requestCounter := &testCounter{}
// 	repo.requestCounter = requestCounter
// 	migrator := NewMigrator(repo)

// 	t.Run("updating cached ref props", testUpdatingCachedRefProps(repo, parkingGaragesSchema()))

// 	// explicitly stop cache indexing to clean up
// 	repo.StopCacheIndexing()
// }
