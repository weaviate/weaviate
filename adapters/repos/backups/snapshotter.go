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

package backups

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/snapshots"
)

type Snapshotter interface { // implemented by the index
	// CreateSnapshot creates a snapshot which is metadata referencing files on disk.
	// While the snapshot exists, the index makes sure that those files are never
	// changed, for example by stopping compactions.
	//
	// The index stays usable with a snapshot present, it can still accept
	// reads+writes, as the index is built in an append-only-way.
	//
	// Snapshot() fails if another snapshot exists.
	CreateSnapshot(ctx context.Context, snapshot *snapshots.Snapshot) (*snapshots.Snapshot, error)

	// ReleaseSnapshot signals to the underlying index that the files have been
	// copied (or the operation aborted), and that it is safe for the index to
	// change the files, such as start compactions.
	ReleaseSnapshot(ctx context.Context, id string) error
}
