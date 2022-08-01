//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/helper"
)

func createObjectClass(t *testing.T, class *models.Class) {
	helper.CreateClass(t, class)
}

func createObject(t *testing.T, object *models.Object) {
	helper.CreateObject(t, object)
}

func createObjectsBatch(t *testing.T, objects []*models.Object) {
	helper.CreateObjectsBatch(t, objects)
}

func deleteObjectClass(t *testing.T, class string) {
	helper.DeleteClass(t, class)
}
