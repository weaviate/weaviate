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

package db

import (
	"os"

	entcfg "github.com/weaviate/weaviate/entities/config"
)

// docIDReuseEnabled gates the docID-reuse feature. When ON, a delete's
// inverted-index cleanup must be made durable (fsynced) BEFORE the object row
// delete, because an orphaned posting would later resolve to a DIFFERENT
// object once its docID is reused. When OFF (the default), deletes keep
// today's cheaper page-cache WAL flush.
//
// Read per call rather than cached: deletes are not so hot that one Getenv
// matters, and tests toggle it via t.Setenv.
func docIDReuseEnabled() bool {
	return entcfg.Enabled(os.Getenv("DOCID_REUSE_ENABLED"))
}
