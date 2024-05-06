//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestGetIndex(t *testing.T) {
	db := testDB(t.TempDir(), []*models.Class{}, make(map[string]*sharding.State))

	// empty indices
	db.indices = map[string]*Index{}
	idx := db.GetIndex(schema.ClassName("test1"))
	require.Nil(t, idx)

	// after 20 ms
	go func() {
		time.Sleep(20 * time.Millisecond)
		db.indexLock.Lock()
		defer db.indexLock.Unlock()
		db.indices = map[string]*Index{
			"test1": {},
		}
	}()
	idx = db.GetIndex(schema.ClassName("test1"))
	require.NotNil(t, idx)

	// after 50 ms
	go func() {
		time.Sleep(50 * time.Millisecond)
		db.indexLock.Lock()
		defer db.indexLock.Unlock()
		db.indices = map[string]*Index{
			"test2": {},
		}
	}()
	idx = db.GetIndex(schema.ClassName("test2"))
	require.NotNil(t, idx)

	// after 100 ms
	go func() {
		time.Sleep(100 * time.Millisecond)
		db.indexLock.Lock()
		defer db.indexLock.Unlock()
		db.indices = map[string]*Index{
			"test3": {},
		}
	}()
	idx = db.GetIndex(schema.ClassName("test3"))
	require.NotNil(t, idx)
}
