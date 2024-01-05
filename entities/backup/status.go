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

package backup

type Status string

const (
	Started      Status = "STARTED"
	Transferring Status = "TRANSFERRING"
	Transferred  Status = "TRANSFERRED"
	Success      Status = "SUCCESS"
	Failed       Status = "FAILED"
)

type CreateMeta struct {
	Path   string
	Status Status
}

type RestoreMeta struct {
	Path   string
	Status Status
}
