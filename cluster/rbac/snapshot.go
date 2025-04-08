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

func (m *Manager) SnapShot() (*authorization.Snapshot, error) {
	// this check because RBAC is optional
	if m.snapshotter == nil {
		return nil, nil
	}
	return m.snapshotter.SnapShot()
}

func (m *Manager) Restore(r io.Reader) error {
	// this check because RBAC is optional
	if m.snapshotter == nil {
		return nil
	}
	return m.snapshotter.Restore(r)
}
