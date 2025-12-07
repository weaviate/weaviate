package multi_node

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestObjectTTLMultiNode(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		With3NodeCluster().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	// send TTL delete requests to a different node, to test cluster-wide propagation
	secondNode := compose.GetWeaviateNode2().DebugURI()

	class := &models.Class{
		Class: "TestingTTL",
		Properties: []*models.Property{
			{
				Name:     "contents",
				DataType: schema.DataTypeText.PropString(),
			},
			{
				Name:     "expireDate",
				DataType: schema.DataTypeDate.PropString(),
			},
		},
		ObjectTTLConfig: &models.ObjectTTLConfig{
			Enabled:    true,
			DeleteOn:   "expireDate",
			DefaultTTL: 60, // 1 minute
		},
		Vectorizer: "none",
	}

	baseTime := time.Now().UTC()
	helper.DeleteClass(t, class.Class)
	defer helper.DeleteClass(t, class.Class)
	helper.CreateClass(t, class)
	for i := 0; i < 11; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			ID:    helper.IntToUUID(uint64(i)),
			Class: class.Class,
			Properties: map[string]interface{}{
				"contents":   "some text",
				"expireDate": baseTime.Add(time.Minute * time.Duration(i)).Format(time.RFC3339),
			},
		}))
	}

	expirationTime := baseTime.Add(5 * time.Minute).Add(time.Second)
	deleteTTL(t, secondNode, expirationTime, false)
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		objs, err := helper.GetObjects(t, class.Class)
		require.NoError(ct, err)
		require.Len(ct, objs, 6) // 0..4 should be deleted => 11 - 5 = 6
	}, time.Second*5, 500*time.Millisecond)
}

func deleteTTL(t *testing.T, node string, deletionTime time.Time, ownNode bool) {
	t.Helper()

	u := url.URL{
		Scheme: "http",
		Host:   node,
		Path:   "/debug/ttl/deleteall",
	}
	q := u.Query()
	q.Set("expiration", deletionTime.UTC().Format(time.RFC3339))
	q.Set("targetOwnNode", strconv.FormatBool(ownNode))
	u.RawQuery = q.Encode()

	resp, err := http.Get(u.String())
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)
}
