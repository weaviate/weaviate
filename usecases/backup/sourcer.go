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

package backup

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/backup"
)

// Sources represents the source of artifacts used in the backup
type Sourcer interface { // implemented by the index
	// CreateBackup creates a snapshot which is metadata referencing files on disk.
	// While the snapshot exists, the index makes sure that those files are never
	// changed, for example by stopping compactions.
	//
	// The index stays usable with a snapshot present, it can still accept
	// reads+writes, as the index is built in an append-only-way.
	//
	// Snapshot() fails if another snapshot exists.
	CreateBackup(ctx context.Context, snapshot *backup.Snapshot) (*backup.Snapshot, error)

	// ReleaseBackup signals to the underlying index that the files have been
	// copied (or the operation aborted), and that it is safe for the index to
	// change the files, such as start compactions.
	ReleaseBackup(ctx context.Context, id string) error
}

type SourceFactory interface {
	SourceFactory(className string) Sourcer
}
