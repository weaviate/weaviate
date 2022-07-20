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

import "time"

type Snapshot struct {
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`

	ID       string   `json:"id"`
	Files    []string `json:"files"`
	BasePath string   `json:"basePath"`
}

// type Backup struct {
// 	Events []BackupEvent
// }

// type BackupEvent struct {
// 	Time time.Time
// 	Msg  string
// }
