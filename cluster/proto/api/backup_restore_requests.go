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

package api

// RestoreRolesAndUsersRequest carries a backup's role and user snapshots in one RAFT
// entry. An empty blob leaves that store untouched. No version field: each
// blob checks its own.
type RestoreRolesAndUsersRequest struct {
	Roles           []byte
	Users           []byte
	StripNamespaces bool
}
