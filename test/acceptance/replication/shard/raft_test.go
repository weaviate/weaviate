package shard

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

const UUID = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241")

func Test_RaftShardReplication(t *testing.T) {
	helper.SetupClient("localhost:8080")

	cls := articles.ParagraphsClass()
	cls.ReplicationConfig = &models.ReplicationConfig{
		Factor:      1,
		RaftEnabled: true,
	}

	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)
	helper.CreateObject(t, articles.NewParagraph().WithContents("RAFT").WithID(UUID).Object())

	// Verify the object eventually exists on all nodes
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, port := range []string{"8080", "8081", "8082"} {
			obj := getObj(t, cls.Class, port)
			assert.NotNil(ct, obj, "Object should exist on node at port %s", port)
		}
	}, 60*time.Second, 1*time.Second)
}

func getObj(t *testing.T, cls, port string) *models.Object {
	helper.SetupClient("localhost:" + port)
	obj, err := helper.GetObject(t, cls, UUID)
	require.NoError(t, err)
	return obj
}
