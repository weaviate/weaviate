//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func mirrorKeyForGen(version uint64) MigrationRecordKey {
	return MigrationRecordKey{
		TaskVersion:  version,
		StrategyCode: StrategyCodeSearchableRetokenize,
		UnitID:       "shard-1__node-0",
	}
}

func TestMigrationMirrorRegistry(t *testing.T) {
	gen10, gen20 := mirrorKeyForGen(10), mirrorKeyForGen(20)

	tests := []struct {
		name      string
		exercise  func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string))
		wantFired map[string]int
		wantArmed int
	}{
		{
			name: "disarming an armed pair runs its handle",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen10, "title")
				r.DisarmMigrationMirror(gen10, "title")
			},
			wantFired: map[string]int{"10/title": 1},
		},
		{
			name: "disarming twice runs it once: every edge that disarms is re-derived at each load",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen10, "title")
				r.DisarmMigrationMirror(gen10, "title")
				r.DisarmMigrationMirror(gen10, "title")
			},
			wantFired: map[string]int{"10/title": 1},
		},
		{
			name: "disarming a pair that was never armed is a no-op",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				r.DisarmMigrationMirror(gen10, "title")
			},
			wantFired: map[string]int{},
		},
		{
			name: "properties of one record are separable",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen10, "title")
				arm(gen10, "body")
				r.DisarmMigrationMirror(gen10, "title")
			},
			wantFired: map[string]int{"10/title": 1},
			wantArmed: 1,
		},
		{
			name: "two generations on one property stay separable while both are armed",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen10, "title")
				arm(gen20, "title")
				r.DisarmMigrationMirror(gen10, "title")
			},
			wantFired: map[string]int{"10/title": 1},
			wantArmed: 1,
		},
		{
			name: "re-arming a pair disarms the handle it replaces",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen10, "title")
				arm(gen10, "title")
			},
			wantFired: map[string]int{"10/title": 1},
			wantArmed: 1,
		},
		{
			name: "a whole record disarms at once without touching another one",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen10, "title")
				arm(gen10, "body")
				arm(gen20, "title")
				r.DisarmMigrationMirrors(gen10)
			},
			wantFired: map[string]int{"10/title": 1, "10/body": 1},
			wantArmed: 1,
		},
		{
			name: "disarming a record that has nothing armed is a no-op",
			exercise: func(r *migrationMirrorRegistry, arm func(MigrationRecordKey, string)) {
				arm(gen20, "title")
				r.DisarmMigrationMirrors(gen10)
			},
			wantFired: map[string]int{},
			wantArmed: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The zero value has to be usable: a shard holds one as a plain
			// field, with no constructor to run.
			var registry migrationMirrorRegistry

			fired := map[string]int{}
			arm := func(key MigrationRecordKey, prop string) {
				label := fmt.Sprintf("%d/%s", key.TaskVersion, prop)
				registry.ArmMigrationMirror(key, prop, func() { fired[label]++ })
			}

			tt.exercise(&registry, arm)

			require.Equal(t, tt.wantFired, fired)
			require.Equal(t, tt.wantArmed, registry.ArmedMigrationMirrors())
		})
	}
}

func TestMigrationMirrorRegistryConcurrentAccess(t *testing.T) {
	var registry migrationMirrorRegistry

	const actors = 8
	var wg sync.WaitGroup
	for i := range actors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := mirrorKeyForGen(uint64(i + 1))
			for round := range 64 {
				registry.ArmMigrationMirror(key, "title", func() {})
				registry.ArmMigrationMirror(key, "body", func() {})
				_ = registry.ArmedMigrationMirrors()
				if round%2 == 0 {
					registry.DisarmMigrationMirror(key, "title")
				} else {
					registry.DisarmMigrationMirrors(key)
				}
			}
			registry.DisarmMigrationMirrors(key)
		}()
	}
	wg.Wait()

	require.Zero(t, registry.ArmedMigrationMirrors())
}
