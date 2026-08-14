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

package clusterprobe_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/clusterprobe"
)

// The wanted values are spelled out as literals on purpose. Comparing a
// constant to itself passes however it is reworded, and a reworded constant is
// exactly the change that disables the gate without failing anything.
func TestWireConstants(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "route path", got: clusterprobe.BackupNodeActivityPath, want: "/backups/node-activity"},
		{name: "payload marker", got: clusterprobe.BackupNodeActivityMarker, want: "weaviate/backup-node-activity"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}
