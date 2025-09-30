//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package acceptance_with_go_client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestUsage(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	getClassName := func(t *testing.T, i int) string {
		return t.Name() + "Class" + fmt.Sprintf("%d", i)
	}
	numClasses := 100

	classNames := make([]string, 0, numClasses)
	c.Schema().AllDeleter().Do(ctx)
	classCreator := c.Schema().ClassCreator()
	// create a bunch of classes
	for i := 0; i < numClasses; i++ {
		className := getClassName(t, i)

		classNames = append(classNames, className)
		c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "first",
					DataType: []string{string(schema.DataTypeText)},
				},
			},
		}
		require.NoError(t, classCreator.WithClass(class).Do(ctx))

	}

	fmt.Println("created classes")
	time.Sleep(5 * time.Second)

	resp, err := http.Get("http://" + "localhost:6060" + "/debug/usage")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotEmpty(t, body)

	for i := 0; i < numClasses; i++ {
		className := classNames[i]
		// concurrent class delete with usage module

		err := c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

		require.NoError(t, err)
		exists, err := c.Schema().ClassExistenceChecker().WithClassName(className).Do(ctx)
		require.NoError(t, err)
		require.False(t, exists)

	}
}

func TestUsageTenants(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := t.Name() + "ClassTenants"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "first",
				DataType: []string{string(schema.DataTypeText)},
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	tenants := make([]models.Tenant, 1000)
	for i := range tenants {
		tenants[i] = models.Tenant{Name: fmt.Sprintf("tenant%d", i)}
	}
	c.Schema().TenantsCreator().WithClassName(className).WithTenants(tenants...).Do(ctx)

	fmt.Println("restart")

	//go func() {
	//        for {
	//                go func() {
	//                        resp, err := http.Get("http://" + "localhost:6060" + "/debug/usage")
	//                        if err != nil {
	//                                return
	//                        }
	//                        if resp.StatusCode == http.StatusOK {
	//                                body, err := io.ReadAll(resp.Body)
	//                                require.NoError(t, err)
	//                                require.NotEmpty(t, body)
	//                                err = resp.Body.Close()
	//                                require.NoError(t, err)
	//                        }
	//                }()
	//                time.Sleep(time.Microsecond * 500)
	//        }
	//}()

	for i := range tenants {
		go func() {
			time.Sleep(time.Millisecond * time.Duration(i))
			resp, err := http.Get("http://" + "localhost:6060" + "/debug/usage")
			if err != nil {
				return
			}
			if resp.StatusCode == http.StatusOK {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.NotEmpty(t, body)
				err = resp.Body.Close()
				require.NoError(t, err)
			}
		}()
		err := c.Schema().TenantsDeleter().WithClassName(className).WithTenants(tenants[i].Name).Do(ctx)
		require.NoError(t, err)
		fmt.Println("deleted tenant", tenants[i].Name)
	}
}
