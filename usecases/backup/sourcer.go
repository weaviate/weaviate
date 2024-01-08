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

import (
	"context"

	"github.com/weaviate/weaviate/entities/backup"
)

// Sourcer represents the source of artifacts used in the backup
type Sourcer interface { // implemented by the index
	// ReleaseBackup signals to the underlying index that the files have been
	// copied (or the operation aborted), and that it is safe for the index to
	// change the files, such as start compactions.
	ReleaseBackup(_ context.Context, id, class string) error

	// Backupable returns whether all given class can be backed up.
	Backupable(_ context.Context, classes []string) error

	// BackupDescriptors returns a channel of class descriptors.
	// Class descriptor records everything needed to restore a class
	// If an error happens a descriptor with an error will be written to the channel just before closing it.
	//
	// BackupDescriptors acquires resources so that a call to ReleaseBackup() is mandatory to free acquired resources.
	BackupDescriptors(_ context.Context, bakid string, classes []string,
	) <-chan backup.ClassDescriptor

	// ClassExists checks whether a class exits or not
	ClassExists(name string) bool

	// ListBackupable returns a list of all classes which can be backed up.
	//
	// A class cannot be backed up either if it doesn't exist or if it has more than one physical shard.
	ListBackupable() []string
}
