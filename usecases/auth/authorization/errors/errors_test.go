//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

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
	expectedErrMsg := "forbidden: user 'john' has insufficient permissions to delete schema/things"
	assert.Equal(t, expectedErrMsg, err.Error())
}

func Test_ForbiddenError_SingleGroup(t *testing.T) {
	principal := &models.Principal{
		Username: "john",
		Groups:   []string{"worstusers"},
	}

	err := NewForbidden(principal, "delete", "schema/things")
	expectedErrMsg := "forbidden: user 'john' (of group 'worstusers') has insufficient permissions to delete schema/things"
	assert.Equal(t, expectedErrMsg, err.Error())
}

func Test_ForbiddenError_MultipleGroups(t *testing.T) {
	principal := &models.Principal{
		Username: "john",
		Groups:   []string{"worstusers", "fraudsters", "evilpeople"},
	}

	err := NewForbidden(principal, "delete", "schema/things")
	expectedErrMsg := "forbidden: user 'john' (of groups 'worstusers', 'fraudsters', 'evilpeople') " +
		"has insufficient permissions to delete schema/things"
	assert.Equal(t, expectedErrMsg, err.Error())
}
