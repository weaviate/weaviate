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

package acceptance_with_go_client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestConcurrentClassAdd(t *testing.T) {
	ctx := context.Background()
	url := "localhost:8080"

	c := client.New(client.Config{Scheme: "http", Host: url})
	_ = c.Schema().AllDeleter().Do(ctx)
	parallelReqs := 200
	wg := sync.WaitGroup{}
	wgStartReqests := sync.WaitGroup{}
	wgStartReqests.Add(parallelReqs)
	wg.Add(parallelReqs)

	for i := 0; i < parallelReqs; i++ {
		go func(j int) {
			classname := fmt.Sprintf("Test_%v", j)

			class := models.Class{
				Class: classname,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: []string{string(schema.DataTypeText)},
					},
				},
			}
			c := client.New(client.Config{Scheme: "http", Host: url})

			err := c.Schema().ClassCreator().WithClass(&class).Do(ctx)
			if err != nil {
				log.Print(err)
			}

			for k := 0; k < 20; k++ {
				_, err := c.Data().Creator().WithClassName(classname).WithProperties(
					map[string]interface{}{"text": string(rune(k))},
				).Do(ctx)
				if err != nil {
					log.Print(err)
				}
			}

			_ = c.Schema().ClassDeleter().WithClassName(class.Class).Do(ctx)
			if err != nil {
				log.Print(err)
			}

			wg.Done()
		}(i)
	}
	wg.Wait()

	// test for deadlock
	_, err := c.Schema().Getter().Do(ctx)
	assert.Nil(t, err)
}
