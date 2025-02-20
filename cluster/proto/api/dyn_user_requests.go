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

package api

const (
	// NOTE: in case changes happens to the dynamic user message, add new version
	DynUserLatestCommandPolicyVersion = iota
)

type CreateUsersRequest struct {
	UserId         string
	SecureHash     string
	UserIdentifier string
	Version        int
}
