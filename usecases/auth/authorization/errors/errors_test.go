package errors

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
)

func Test_ForbiddenError_NoGroups(t *testing.T) {
	principal := &models.Principal{
		Username: "john",
	}

	err := NewForbidden(principal, "delete", "schema/things")
	expectedErrMsg := "forbidden: user 'john' does not have permissions to delete schema/things"
	assert.Equal(t, expectedErrMsg, err.Error())
}

func Test_ForbiddenError_SingleGroup(t *testing.T) {
	principal := &models.Principal{
		Username: "john",
		Groups:   []string{"worstusers"},
	}

	err := NewForbidden(principal, "delete", "schema/things")
	expectedErrMsg := "forbidden: user 'john' (of group 'worstusers') does not have permissions to delete schema/things"
	assert.Equal(t, expectedErrMsg, err.Error())
}

func Test_ForbiddenError_MultipleGroups(t *testing.T) {
	principal := &models.Principal{
		Username: "john",
		Groups:   []string{"worstusers", "fraudsters", "evilpeople"},
	}

	err := NewForbidden(principal, "delete", "schema/things")
	expectedErrMsg := "forbidden: user 'john' (of groups 'worstusers', 'fraudsters', 'evilpeople') " +
		"does not have permissions to delete schema/things"
	assert.Equal(t, expectedErrMsg, err.Error())
}
