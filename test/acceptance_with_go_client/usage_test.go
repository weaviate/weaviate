package acceptance_with_go_client

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
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

	classCreator := c.Schema().ClassCreator()
	classCreateWG := sync.WaitGroup{}
	classCreateWG.Add(numClasses)
	// create a bunch of classes
	for i := 0; i < numClasses; i++ {
		className := getClassName(t, i)

		classNames = append(classNames, className)
		go func() {
			defer classCreateWG.Done()
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
		}()

	}
	classCreateWG.Wait()

	fmt.Println("created classes")
	rand.Shuffle(len(classNames), func(i, j int) {
		classNames[i], classNames[j] = classNames[j], classNames[i]
	})

	for i := 0; i < numClasses; i++ {
		className := classNames[i]
		// concurrent class delete with usage module
		wg := sync.WaitGroup{}
		wg.Add(1)
		allDeleted := atomic.Bool{}
		go func() {
			defer wg.Done()
			defer allDeleted.Store(true)
			err := c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
			if err != nil {
				fmt.Println("error deleting class:", err)
			}
			require.NoError(t, err)
			exists, err := c.Schema().ClassExistenceChecker().WithClassName(className).Do(ctx)
			require.NoError(t, err)
			require.False(t, exists)
		}()

		for {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := http.Get("http://" + "localhost:6060" + "/debug/usage")
				require.NoError(t, err)
				if resp.StatusCode == http.StatusOK {
					body, err := io.ReadAll(resp.Body)
					require.NoError(t, err)
					require.NotEmpty(t, body)
					err = resp.Body.Close()
					require.NoError(t, err)
				}
			}()
			time.Sleep(time.Millisecond)
			if allDeleted.Load() {
				return
			}
		}

		wg.Wait()
	}
}
