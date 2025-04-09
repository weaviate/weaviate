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

package rbac

import (
	"io"

	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (m *Manager) Snapshot() (*authorization.Snapshot, error) {
	return m.snapshotter.Snapshot()
}

func (m *Manager) Restore(r io.Reader) error {
	return m.snapshotter.Restore(r)
}
