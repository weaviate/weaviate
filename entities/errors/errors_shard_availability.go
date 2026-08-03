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

// ErrShardBackupProtected marks a shard whose files a running backup still
// has to upload; activating it would move them out from under the upload.
// It clears on every backup exit path, does not survive a restart, and
// transports must map it to a retryable status, not a server error.
var ErrShardBackupProtected = errors.New("protected for backup, activation blocked")
