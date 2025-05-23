package schema

import (
	"testing"

	"github.com/weaviate/weaviate/test/docker"
)

func TestSharedSchema(t *testing.T, compose *docker.DockerCompose) {
	t.Parallel()

	t.Run("TestInvalidDataTypeInProperty", TestInvalidDataTypeInProperty)
	t.Run("TestInvalidPropertyName", TestInvalidPropertyName)
	t.Run("TestAddAndRemoveObjectClass", TestAddAndRemoveObjectClass)
	t.Run("TestUpdateHNSWSettingsAfterAddingRefProps", TestUpdateHNSWSettingsAfterAddingRefProps)
	t.Run("TestUpdateClassWithoutVectorIndex", TestUpdateClassWithoutVectorIndex)
	t.Run("TestUpdateDistanceSettings", TestUpdateDistanceSettings)
	t.Run("TestGetClassWithConsistency", func(t *testing.T) { TestGetClassWithConsistency(t, compose) })
	t.Run("TestUpdateClassDescription", TestUpdateClassDescription)
	t.Run("TestUpdatePropertyDescription", TestUpdatePropertyDescription)
	t.Run("TestUpdateClassWithText2VecOpenAI", TestUpdateClassWithText2VecOpenAI)
}
