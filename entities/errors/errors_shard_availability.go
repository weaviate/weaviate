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

package errors

import "errors"

// ErrShardBackupProtected marks a shard that a running backup keeps cold
// because it still has to upload files that live in the shard's own directory.
// Activating the shard would move them out from under the upload.
//
// It clears itself when the backup finishes, on every exit path, and does not
// survive a restart, so transports must map it to a retryable status rather
// than a server error.
var ErrShardBackupProtected = errors.New("protected for backup, activation blocked")
