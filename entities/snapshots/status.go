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

package snapshots

const (
	CreateStarted      CreateStatus = "STARTED"
	CreateTransferring CreateStatus = "TRANSFERRING"
	CreateTransferred  CreateStatus = "TRANSFERRED"
	CreateSuccess      CreateStatus = "SUCCESS"
	CreateFailed       CreateStatus = "FAILED"
)

const (
	RestoreStarted      RestoreStatus = "STARTED"
	RestoreTransferring RestoreStatus = "TRANSFERRING"
	RestoreTransferred  RestoreStatus = "TRANSFERRED"
	RestoreSuccess      RestoreStatus = "SUCCESS"
	RestoreFailed       RestoreStatus = "FAILED"
)

type (
	CreateStatus  string
	RestoreStatus string
)

type CreateMeta struct {
	Path   string
	Status CreateStatus
}

type RestoreMeta struct {
	Path   string
	Status RestoreStatus
}
